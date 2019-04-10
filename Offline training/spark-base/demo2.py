import sys
from pyspark import SparkContext

if __name__ == '__main__':
	file = sys.argv[1]

	sc = SparkContext(appName = "demo2")
	#现在想用每一个word作为element
	#首先用split进行分割，然后用distinct()进行去重
	data = sc.textFile(file).flatMap(lambda line: line.split(' ')).distinct()
	data.saveAsTextFile("demo2_output")
	#最后可以查看输出结果
	sc.stop()