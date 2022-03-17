def singleton(cls):
    _instance = {}

    def get_instance():
        if cls not in _instance:
            _instance[cls] = cls()
        return _instance[cls]

    return get_instance
