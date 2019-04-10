import sys
from pyspark import SparkContext

if __name__ == '__main__':
	file = sys.argv[1]

	sc = SparkContext(appName = "demo1")
	#首先生成一个rdd文件，然后又进行了map，lamda函数的意思就是对于每一个element进行了upper()操作
	data_uc = sc.textFile(file).map(lambda line: line.upper())
	#然后存储
	#data_uc.saveAsTextFile("demo_upper_output")
	#这里用了刚刚的rdd，然后进行filter
	#这是因为rdd是immutable的
	#这个意思就是说，如果是T开头的，那就过滤下来
	data_filt = data_uc.filter(lambda line: line.startswith("T"))
	#这里的保存文件应该和上面的不一样
	#这是因为上面的输出已经有了一个success文件了
	data_filt.saveAsTextFile("demo_output")

	sc.stop()