import pandas as pd
class Source_Hub:
    # def __init__(self, source):
    #     self.source = source

    # 简单工厂模式
    # 如果一定要实现标准工厂模式可能需要递归遍历深层子类
    @classmethod
    def create(cls, source_type, *args):
        for subclass in cls.__subclasses__():
            source_type ='Source'+source_type
            if subclass.__name__.lower() == source_type.lower():
                return subclass(*args)
        raise ValueError(f"Unknown source type: {source_type}")

    def get_data(self):  #接口
        pass



