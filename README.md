# Elotl ETL
A basic ETL implementation for python

## Sequential mode
- You have to specify the list of tasks in a sequential way
- All the tasks will be executed after the previous one will be in "success" state
- If some task failed... the whole pipeline failed
- If you want to continue processing even taught one task is failing, you have to specify in the config object: "on_failure_behaivor": "continue"
- You don't need to specify any depends on array, because your dependencies are implicit defined (the tasks preceding the actual job)

