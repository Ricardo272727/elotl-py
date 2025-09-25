class MetadataManager:
    DEBUG = True
    @staticmethod
    def extract_dependencies(target):
        return target.__annotations__.get('dependencies', [])

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

