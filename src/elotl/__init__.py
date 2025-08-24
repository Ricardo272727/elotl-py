import time

def needsSuccess(name:str):
    return name.endswith('.success')

def needsFinished(name:str):
    return name.endswith('.finished')

def needsError(name:str):
    return name.endswith('.error')

def validate_dependencies(deps):
    for dep in deps:
        if type(dep) != str:
            raise ValueError('Dependencies array must only contain strings')
        if '.' in dep and not any([needsSuccess(dep), needsFinished(dep), needsError(dep)]):
            raise ValueError('Dependency name must end with .[status: success|error|finished]')

class MetadataManager:
    DEBUG = True
    @staticmethod
    def extract_dependencies(target):
        return target.__annotations__['dependencies']

    @staticmethod
    def extract_name(target):
        return target.__annotations__['step_name']

    @staticmethod
    def add(fn, data):
        current = {}
        if hasattr(fn, '__annotations__'):
            current = fn.__annotations__
        current.update(**data)
        fn.__annotations__ = current
        return fn

def step(fn):
    MetadataManager.add(fn, { 'step_name': fn.__name__ })
    return fn 

def depends(dependencies = []):
    def inner(fn):
        validate_dependencies(dependencies) 
        return fn
    MetadataManager.add(inner, { 'dependencies': dependencies })
    return inner

class Modes:
    SEQUENTIAL = 'sequential'
    ASYNC_DAG = 'async_dag'
    PARALLEL = 'parallel'

class Timer:
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.duration = None
    
    def start(self):
        self.start_time = time.time() 
    
    def stop(self):
        self.end_time = time.time()

    def get_value(self):
        if not self.start_time or not self.end_time:
            raise ValueError("Invalid timer state")
        return (self.end_time - self.start_time) * 1000

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

executors = {
    Modes.SEQUENTIAL: SequentialExecutor,
    Modes.ASYNC_DAG: None,
    Modes.PARALLEL: None,
}

def generate_report(context, steps, config, results):
    from pprint import pprint
    for result in results:
        print(result)

def execute(context:dict = {}, steps:list = [], config={}):
    # execute
    Executor = executors.get(config.get('mode'))
    executor : ExecutorBase = Executor(context, steps, config)
    results = executor.execute()
    # generate report
    generate_report(context, steps, config, results)
