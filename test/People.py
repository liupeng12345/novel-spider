class People:
    __num__ = 0

    def __init__(self):
        pass


if __name__ == '__main__':
    p1 = People()
    p2 = People()
    p1.__num__ += 1
    print(p1.__num__)
    print(People.__num__)
