import asyncio
from .reports import generate_report
from .settings import ASYNC_DAG, SEQUENTIAL, PARALLEL
from .base_executor import ExecutorBase
from .sequential_executor import SequentialExecutor
from .async_dag_executor import AsyncDagExecutor

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
