# WordCount-PySpark
Running my first pyspark app in CDH5

## PySpark Job in Spark UI
[![screenshot_1509487656.png](https://s19.postimg.org/4sxl9ku9v/screenshot_1509487656.png)](https://postimg.org/image/clo91k08v/)

Since Spark follows *Lazy Evaluation* it created a single **action** job in the end.
It waits until a action is called.


## Details for 1st Job

[![screenshot_1509488132.png](https://s19.postimg.org/b7wm67yer/screenshot_1509488132.png)](https://postimg.org/image/e1zrjo0kv/)

So this **Job** was further divided into two stages, one for Stage0: reduceByKey and the other for Stage1: collect(our last code).
Since we used reduceByKey it 
