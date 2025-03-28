class test:
    @staticmethod
    def temp(a):
        return a

def add(a, b):
    return a + b

if __name__ == '__main__':
    print(add(test.temp(5),test.temp(2)))