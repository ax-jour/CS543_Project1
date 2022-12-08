import pandas as pd
from sklearn.utils import shuffle


if __name__ == '__main__':
    df = pd.read_csv("./output/processed_int/part-00000-58cb0718-851a-45a6-bc50-3346e8bdcbae-c000.csv", header=None)
    total = len(df)
    split = total // 10
    split *= 9
    df = shuffle(df)
    df[:split].to_csv("./data/train.csv", header=False, index=False)
    df[split:].to_csv("./data/test.csv", header=False, index=False)