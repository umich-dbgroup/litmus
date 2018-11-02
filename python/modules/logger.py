import sys

class Logger:
    def __init__(self, path):
        self._path = path
    def __enter__(self):
        sys.stdout = open(self._path, 'w', 0)
        return self
    def __exit__(self, exec_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = sys.__stdout__
