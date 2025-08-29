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

class ExecutorBase:
    def __init__(self, context:dict = {}, steps:list=[], config:dict={}):
        self.context = context
        self.steps = steps
        self.config = config
        self.timers = {}
        self.results = []
    
    def execute(self) -> list[StepResult]:
        raise NotImplementedError() 

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
            # setup
            step_name = MetadataManager.extract_name(step)
            error = None
            result = None
            # execute
            try:
                self.start_timer(step_name)
                result = step(self.context)
            except Exception as e:
                error = e                
            finally:
                self.stop_timer(step_name)

            # fail if config is "stop"
            if on_failure == 'stop' and error:
                raise error

            # save results in context
            self.add_result(step_name, result)
        return self.results

class AsyncDagExecutor(ExecutorBase):
    def execute(self):
        # validate acyclic graph
        # while not all completed or timeout reached 
        #  execute jobs that have dependencies completed
        #  add results to the context
        #  
        pass        

executors = {
    SEQUENTIAL: SequentialExecutor,
    ASYNC_DAG: None,
    PARALLEL: None,
}

def execute(context:dict = {}, steps:list = [], config={}):
    # execute
    Executor = executors[config['mode']]
    executor : ExecutorBase = Executor(context, steps, config)
    results = executor.execute()
    # generate report
    generate_report(context, steps, config, results)
