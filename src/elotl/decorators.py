from .metadata import MetadataManager

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

def step(fn):
    MetadataManager.add(fn, { 'step_name': fn.__name__ })
    return fn 

def depends(dependencies = []):
    def inner(fn):
        validate_dependencies(dependencies) 
        return fn
    MetadataManager.add(inner, { 'dependencies': dependencies })
    return inner
