# PySpark- WordCount, under the hood!
Running my first pyspark app in CDH5
___

## PySpark Job in Spark UI
[![screenshot_1509487656.png](https://s19.postimg.org/4sxl9ku9v/screenshot_1509487656.png)](https://postimg.org/image/clo91k08v/)

Since Spark follows *Lazy Evaluation* it created a single **action** job in the end.
It waits until a action is called.


## Details for 1st Job

[![screenshot_1509488132.png](https://s19.postimg.org/b7wm67yer/screenshot_1509488132.png)](https://postimg.org/image/e1zrjo0kv/)

So this **Job** was further divided into two stages, one for Stage0: reduceByKey and the other for Stage1: collect(our last code).
We used reduceByKey in it,so in future try to minimize the number of shuffles and the amount of data shuffled.
Shuffles are expensive operations; all shuffle data must be written to disk and then transferred over the network.

## DAG for Stage1: Collect

[![screenshot_1509492181.png](https://s19.postimg.org/iy7zoqnfn/screenshot_1509492181.png)](https://postimg.org/image/9qfr81gdb/)


[![screenshot_1509488721.png](https://s19.postimg.org/tq0yqct0j/screenshot_1509488721.png)](https://postimg.org/image/lxaaydn1b/)

This clearly show a PythonRDD object is a **scala** object under the hood. It reads/takes the 2.1 KB of shuffled data, **map** it together and returns a **RDD**.


___
___

# Method 2:
### Using .reduceByKey(add)instead of .reduceByKey(lambda w1,w2: w1 + w2)

```python
from operator import add

count=myfile.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(add)

count.collect()

```

### A good defination for flatMap I found on [Stackoverflow](https://stackoverflow.com/questions/22350722/can-someone-explain-to-me-the-difference-between-map-and-flatmap-and-what-is-a-g "Stack Overflow page")

### Later I tried to run it multiple time
[![screenshot_1509567975.png](https://s19.postimg.org/u81mmvdmb/screenshot_1509567975.png)](https://postimg.org/image/4chw3obsf/)

### What if I skip the reduceByKey() and jump on to collect()
[![screenshot_1509568027.png](https://s19.postimg.org/korxsu5df/screenshot_1509568027.png)](https://postimg.org/image/3o91k5sbz/)

### It will skip the reducing part from DAG and read the shuffled data and displays output
[![screenshot_1509568064.png](https://s19.postimg.org/nvmhclfkj/screenshot_1509568064.png)](https://postimg.org/image/77uza3ksv/)


