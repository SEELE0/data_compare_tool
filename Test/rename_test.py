import pandas as pd
df = pd.DataFrame([['A','Atest',123],
['B','Btest',456],
['C','Ctest',789]
],columns=['Site','Test','Price']
                  )

def show(df):
    df = df.rename(columns=lambda x: x.lower())
    return df

def show2(df):
    df.rename(columns=lambda x: x.lower(),inplace=True)
    return df




if __name__ == '__main__':
    print(show(df))
    print(df)
    print(show2(df))
    print(df)