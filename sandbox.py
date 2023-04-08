import pandas as pd

testcase1 = pd.DataFrame(
    {
        "a": [1, 2, 3, 4, 5],
        "b": [1.1, 2.1, 2.2, 2.3, 2.4],
        "c": ["a", "b", "c", "d", None],
        "d": [1, None, None, None, 2],
        "t": [0, 1, 1, 1, 0],
    }
)

if __name__ == "__main__":
    [print(v) for k, v in testcase1.items()]
