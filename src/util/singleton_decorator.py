def singleton(cls, *args, **kw):
    __instance = {}

    def get_instance():
        if cls not in __instance:
            __instance[cls] = cls(*args, **kw)
        return __instance[cls]

    return get_instance
