import asyncio
from typing import List, Union

from .metrics import Timer
from .metadata import MetadataManager
from .reports import generate_report
from .settings import ASYNC_DAG, SEQUENTIAL, PARALLEL

class StepResult:
    def __init__(self, data, metrics={}):
        self.data = data
        self.metrics = metrics

    def __str__(self):
        return f"{self.data} - {self.metrics}"


class Step:
    def __init__(self, name, fn, dependencies):
        self.name = name
        self.fn = fn
        self.dependencies = set(dependencies)

def steps_to_dict(steps):
    steps_dict = {}
    for step in steps:
        steps_dict[step.name] = step
    return steps_dict

def build_steps(steps) -> List[Step]:
    step_list = []
    for step in steps:
        name = MetadataManager.extract_name(step)
        dependencies = MetadataManager.extract_dependencies(step)
        step_list.append(Step(name, step, dependencies))
    return step_list

class ExecutorBase:
    def __init__(self, context=None, steps=None, config=None):
        if config is None:
            config = {}
        if steps is None:
            steps = []
        if context is None:
            context = {}
        self.context = context
        self.steps = build_steps(steps)
        self.steps_dict = steps_to_dict(self.steps)
        self.config = config
        self.timers = {}
        self.results = []
    
    def execute(self) -> List[StepResult]:
        raise NotImplementedError()

    def execute_step(self, step):
        error = None
        result = None
        try:
            self.start_timer(step.name)
            result = step.fn(self.context)
        except Exception as e:
            error = e
        finally:
            self.stop_timer(step.name)

        # save results/errors in context
        if result is not None:
            self.add_result(step.name, result)
        if error is not None:
            self.add_result(step.name, error)
        return result, error

    async def execute_step_async(self, step):
        return self.execute_step(step)

    def start_timer(self, name:str):
        if name in self.timers:
            raise ValueError(f"Timer with name {name} already exists")
        self.timers[name] = Timer() 
        self.timers[name].start()

    def stop_timer(self, name:str):
        if not name in self.timers:
            raise ValueError(f"Timer {name} does not exists")
        self.timers[name].stop()

    def add_result(self, step_name, data):
        sr = StepResult(data, {
            'execution_time': self.timers[step_name].get_value(),
            'start_time': self.timers[step_name].start_time,
            'end_time': self.timers[step_name].end_time,
        })
        self.context[step_name] = data 
        self.results.append(sr)

class SequentialExecutor(ExecutorBase):
    def execute(self):
        on_failure = self.config.get('on_failure_behaivor', 'stop')
        # iterate through the functions...
        for step in self.steps:
            # execute
            result, error = self.execute_step(step)
            # fail if config is "stop"
            if on_failure == 'stop' and error:
                raise error

        return self.results

class AsyncDagExecutor(ExecutorBase):
    def find_root(self) -> Union[Step|None]:
        for name, step in self.steps_dict.items():
            if len(step.dependencies) == 0:
                return step
        return None

    def dfs(self, step:Step, visited:set, stack:set):
        if step.name in stack:
            return True
        if step.name in visited:
            return False

        visited.add(step.name)
        stack.add(step.name)
        print('Dfs')
        for dependency in step.dependencies:
            if self.dfs(self.steps_dict[dependency], visited, stack):
                return True

        stack.remove(step.name)
        return False

    def has_cycle(self, steps):
        visited = set()
        stack = set()
        for step in steps:
            if self.dfs(step, visited, stack):
                return True
        return False

    def validate_acyclic_graph(self, steps):
        # find root
        root = self.find_root()
        if not root:
            raise ValueError("Invalid steps, it would exists at least one step without dependencies")
        print('Checkign cyclic graph')
        if self.has_cycle(self.steps):
            raise ValueError("Cyclic steps detected")

    async def execute(self):
        # validate acyclic graph
        self.validate_acyclic_graph(self.steps)
        completed = set()
        tasks = {}
        print('Executing steps')
        while len(completed) < len(self.steps):
            for step in self.steps:
                valid_deps = len(step.dependencies) == 0 or step.dependencies.issubset(completed)
                running = step.name in tasks
                task_done = step.name in completed
                if not running and not task_done and valid_deps:
                    print(f'Running step: {step.name}')
                    task = asyncio.create_task(self.execute_step_async(step))
                    tasks[step.name] = task
            if not tasks:
                print('Waiting for tasks')
                await asyncio.sleep(0.1)

            done = [name for name, task in tasks.items() if task.done()]
            for name in done:
                print('Completed: ', name)
                completed.add(name)
                del tasks[name]

            await asyncio.sleep(0.1)

        return self.results

executors = {
    SEQUENTIAL: SequentialExecutor,
    ASYNC_DAG: AsyncDagExecutor,
    PARALLEL: None,
}

def execute(context:dict = {}, steps:list = [], config={}):
    # execute
    Executor = executors[config['mode']]
    executor : ExecutorBase = Executor(context, steps, config)
    if config['mode'] == ASYNC_DAG:
        results = asyncio.run(executor.execute())
    else:
        results = executor.execute()
    # generate report
    generate_report(context, steps, config, results)
