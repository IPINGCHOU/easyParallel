import pytest
import easyParallel
import pandas as pd
import time
import math

from functools import reduce
from collections.abc import MutableMapping
from random import randint

def test_simpleList():
    
    input = [1,2,3,4,5,6,7,8]

    def square(x):
        return x**2
    
    pm = easyParallel.ParallelManager()
    testa = pm.do_parallel(input, square, 4)
    print("testa out: ", testa)

    assert testa == [square(i) for i in input]

def test_outputPd():
    
    input = [1,2,3,4]

    def give_Series(x):
        temp = [x, x**2, x**3]
        return pd.DataFrame([temp],
                            columns = ["ori", "square", "tri"])
    
    pm = easyParallel.ParallelManager()
    testa = pm.do_parallel(input, give_Series, 4)
    testa = pd.concat(testa).reset_index(drop = True)

    print("testa out: ", testa)
    
    match = [[1,1,1], [2,4,8], [3,9,27], [4,16,64]]
    match = pd.DataFrame(match,
                         columns = ["ori", "square", "tri"])

    assert testa.equals(match)

def test_improved():
    # test a time costy func
    def task_func(x):
        # something takes time
        randomizer = randint(1000, 10000)
        for j in range(100):
            for i in range(randomizer):
                algo_seed = math.sqrt(math.sqrt(i * randomizer) % randomizer)

        return x
    
    input = list(range(200))

    # for loop
    res1 = []
    start = time.time()
    for i in input:
        res1.append(task_func(i))
    print("For loops - time used: ", time.time() - start)

    # do_parallel
    start = time.time()
    pm = easyParallel.ParallelManager(verbose = True, update_freq = 0.2)
    print("Init used: ", time.time() - start)
    testa = pm.do_parallel(input, task_func, 12)
    print("Parallel - time used: ", time.time() - start)

    assert res1 == testa


