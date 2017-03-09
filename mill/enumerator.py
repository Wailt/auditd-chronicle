class enumerator:
    def __init__(self):
        self.count = 0
        self.data = dict()
        self.inverse_data = dict()
        self.data[(-1,)] = -1
        self.invese_data[-1] = (-1,)

    def get_number(self, item):
        if item not in self.data:
            self.count += 1
            self.data[item] = self.count
            self.inverse_data[self.count] = item
        return self.data[item]