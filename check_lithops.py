from lithops import FunctionExecutor

executor = FunctionExecutor()

def foo(x):
    return x+1


fts = executor.map(foo, range(10))
executor.get_result(fts)