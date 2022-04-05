import os


def mkdir_p(path):
    return os.makedirs(path, exist_ok=True)
