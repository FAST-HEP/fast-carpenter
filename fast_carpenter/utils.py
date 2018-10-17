import os
import sys


if sys.version_info < (3, 2):
    import errno

    def mkdir_p(path):
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise
else:
    def mkdir_p(path):
        return os.makedirs(path, exist_ok=True)
