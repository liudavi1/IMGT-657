from functools import reduce

def groupByKey(data):
    result = dict()
    for key, value in data:
        if key in result:
            result[key].append(value)
        else:
            result[key] = [value]
        print(key,"B")    
        print(result, "A")    
    return result
        
def reduceByKey(f, data):
    key_values = groupByKey(data)
    #print(key_values[key], "B")

    return list(map(lambda key: 
                   (key, reduce(f, key_values[key])), 
                       key_values))
    



data = list(map(lambda x: (x, 1), "hello world HELLO WORLD HeLlO wOrLd".split()))
data

groupByKey(data)
reduceByKey(lambda x, y: x + y, data)
