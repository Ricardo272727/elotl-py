class EtlStep:
    def __init__(self, fn, dependencies=[]):
        self.fn = fn
        self.dependencies = dependencies


def step(fn):
    add_to_annotations(fn, 'etl-step')
    return fn 

def add_to_annotations(fn, **data):
    current = {}
    if hasattr(fn, '__annotations__'):
        current = fn.__annotations__
    current.update(**data)
    fn.__annotations__ = current
    return fn