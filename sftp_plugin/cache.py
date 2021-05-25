from os.path import basename as path_basename, dirname as path_dirname


class Cache():
    @classmethod
    def put(cls, path, attr, value):
        cls._cache.setdefault(attr, {}).setdefault(path_dirname(path), {})[path_basename(path)] = value

    @classmethod
    def get(cls,path, attr, default=None):
        try:
            return cls._cache[attr][path_dirname(path)][path_basename(path)]
        except KeyError:
            return default

    @classmethod
    def clear(cls,path, attr, only_content=False):
        try:
            if only_content:
                cls._cache[attr][path] = {}
            else:
                del cls._cache[attr][path_dirname(path)][path_basename(path)]
        except KeyError:
            pass

    @classmethod
    def pop(cls,path, attr, default=None):
        try:
            return cls._cache[attr][path_dirname(path)].pop(path_basename(path), default)
        except KeyError:
            return default


class SftpCache(Cache):
    _cache = {}


class FtpCache(Cache):
    _cache = {}
