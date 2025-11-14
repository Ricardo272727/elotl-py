from typing import List, Union

from .metrics import Timer
from .metadata import MetadataManager

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

