import asyncio
from typing import Union
from .base_executor import ExecutorBase
from .base_executor import Step

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

