from .base_executor import ExecutorBase


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

