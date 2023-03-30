import inspect
import sys
import mlopslite


if __name__ == "__main__":

    clsmembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
    print(clsmembers)
