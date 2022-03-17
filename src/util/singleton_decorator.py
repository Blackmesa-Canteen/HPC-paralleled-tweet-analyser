def singleton(cls, *args, **kw):
    _instance = {}

    def get_instance():
        if cls not in _instance:
            _instance[cls] = cls(*args, **kw)
        return _instance[cls]

    return get_instance
