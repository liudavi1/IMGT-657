import dask
from dask import delayed
import graphviz
from time import sleep

## calculate sum of the number in a list
def calculate_sum_of_list(x):
    sleep(1)
    y = sum(x)
    return y

## calculate multiplication of two numbers
def get_multiplication(a,b):
    sleep(1)
    y = a * b
    return ycalculate_sum_of_list

n1_list = [2,4,6,8]
n2_list = [1,3,5,7]

x = delayed(calculate_sum_of_list)(n1_list)   # delay calculate n1_list

y = delayed(calculate_sum_of_list)(n2_list)   # delay calculate n2_list


z = delayed(get_multiplication)(x, y)          # delay calculate multiplication
   
print(z)

## visualize the task graph
z.visualize()
