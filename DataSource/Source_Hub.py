class Source_Hub:
    def __init__(self, source):
        self.source = source
    def red_config_file(self):
        return 0
    def get_data(self):
        return self.source.get_data()
    def find_same_colname(self):
        return 0