## import dask dependencies
import dask
from dask import delayed
import graphviz

## import dependencies
from time import sleep
## calculate square of a number

def calculate_mod(x):
    sleep(1)
    x= x**10
    return x

## calculate sum of two numbers
def get_mod(a,b):
    sleep(1)
    return a%b

## Call above functions in a for loop
output = []

## iterate over values and calculate the sum
for i in range(10):
    a = delayed(calculate_mod)(i)
    b = delayed(calculate_mod)(i+10)
    c = delayed(get_mod)(a, b)
    output.append(c)
    
total = dask.delayed(sum)(output)

print(total)
#Visualizing the graphimport 
total.visualize()
