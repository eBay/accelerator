class AutoFlush:
    def __init__(self, fh):
        self.fh = fh
    def write(self, data):
        self.fh.write(data)
        self.fh.flush()
    def flush(self):
        pass
