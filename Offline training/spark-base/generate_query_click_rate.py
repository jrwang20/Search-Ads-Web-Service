import sys
from pyspark import SparkContext

if __name__ == '__main__':
	file = sys.argv[1] #raw train file

	sc = SparkContext(appName="auery_ad_pair_ctr")
	#首先读入file，首先要clean一下用strip()，然后encoding，然后根据逗号split
	data = sc.textFile(file).map(lambda line: line.strip().strip("\n").encode("utf8", "ignore").split(','))
	#现在每一个line都变成了一个array，RDD里就是一个一个field，然后就可以建立一个key-value pair
	#key就希望是adId和query的组合，value就是最后一列的是否click的这一列，分别是3，4列和7列
	#groupByKey这里在用，和reduceByKey的区别就是，reduce是两两相加，而group是希望把group内部的全都加起来
	#在group后面的操作中，把属于这个group内部的所有values进行的操作，首先对所有value求和，然后除以value本身的长度（就是value数组的长度）
	#就比如说，现在一个group里面有12个key-value pair，就是说某一个key出现了12词，其中value有5个0和7个1，就是说广告出现了12次，点击了7次，那么ctr就是7/12
	query_ad_pair_ctr = data.map(lambda fields: (fields[3] + "_" + fields[4], int(fields[7]))).groupByKey().map(lambda (k, values): (k, sum(values) * 1.0/len(values)))

	query_ad_pair_ctr.saveAsTextFile("query_ad_pair_sample_ctr")

	sc.stop()