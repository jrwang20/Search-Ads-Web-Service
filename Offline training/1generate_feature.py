import sys
from pyspark import SparkContext

def process_query(query):
	fields = query.split(" ")
	output = "_".join(fields)
	return output

#Device IP, Device id, Session id, Query, AdID, CampaignID, Ad_category_Query_category(0/1), clicked(0/1)

if __name__ == '__main__':
	# input就是search log，也就是click log，从command line输入
	file = sys.argv[1] 
	#首先创建一个SparkContext
	sc = SparkContext(appName = "CTR_Features")
	output_dir = "/spark/cs209"
	#然后创建RDD，然后进行map，对每一行根据“,”去split开，这样就变成了array了，这个array实际上就是下面的那些fields，而现在的data这个RDD就是由各种array组成
	data = sc.textFile(file).map(lambda line: line.encode("utf8", "ignore").split(','))
	#count feature下面开始用mapreduce计算feature对应的值
	#device_ip是第0列，要统计在第7列，现在是想count，算出当前的device_ip历史上总共有多少次clicked，所以用reduceByKey，key就是device_ip，value就是把所有的加起来(click是1，不click是0，加起来就是click总数)
	device_ip_click = data.map(lambda fields: (fields[0], int(fields[7]))).reduceByKey(lambda v1, v2: v1 + v2)
	#imperssion是指出现的次数，不管是1还是0都要加起来，所以key和原来一样，value就都算做1
	device_ip_impression = data.map(lambda fileds: (fields[0], 1)).reduceByKey(lambda v1, v2: v1 + v2)
	#下面的都一样，类似的
	device_id_click = data.map(lambda fields: (fields[1], int(fields[7]))).reduceByKey(lambda v1, v2: v1 + v2)
	device_id_impression = data.map(lambda fileds: (fields[1], 1)).reduceByKey(lambda v1, v2: v1 + v2)

	ad_id_click = data.map(lambda fields: (fields[4], int(fields[7]))).reduceByKey(lambda v1, v2: v1 + v2)
	ad_id_impression = data.map(lambda fileds: (fields[4], 1)).reduceByKey(lambda v1, v2: v1 + v2)

	#这里的query_campaign_id，实际上就是统计一下两个的组合，就是把query和campaign放在一起，然后进行reduce，其他的和上面一样
	query_campaign_id_click = data.map(lambda fields: (process_query(fields[3]) + "_" + fields[5], int(fields[7]))).reduceByKey(lambda v1, v2: v1 + v2)
	query_campaign_id_impression = data.map(lambda fileds: (process_query(fields[3]) + "_" + fields[5], 1)).reduceByKey(lambda v1, v2: v1 + v2)

	#这里和上面的组合一样的原理
	query_ad_id_click = data.map(lambda fields: (process_query(fields[3]) + "_" + fields[4], int(fields[7]))).reduceByKey(lambda v1, v2: v1 + v2)
	query_ad_id_impression = data.map(lambda fileds: (process_query(fields[3]) + "_" + fields[4], 1)).reduceByKey(lambda v1, v2: v1 + v2)

	#然后就是保存路径了
	device_id_click.saveAsTextFile(output_dir + "demo_device_id_click")
	device_id_impression.saveAsTextFile(output_dir + "demo_device_id_impression")

	device_ip_click.saveAsTextFile(output_dir + "demo_device_ip_click")
	device_ip_impression.saveAsTextFile(output_dir + "demo_device_ip_impression")

	ad_id_click.saveAsTextFile(output_dir + "demo_ad_id_click")
	ad_id_impression.saveAsTextFile(output_dir + "demo_ad_id_impression")

	query_campaign_id_click.saveAsTextFile(output_dir + "demo_query_campaign_id_click")
	query_campaign_id_impression.saveAsTextFile(output_dir + "demo_query_campaign_id_impression")

	query_ad_id_click.saveAsTextFile(output_dir + "demo_query_ad_id_click")
	query_ad_id_impression.saveAsTextFile(output_dir + "demo_query_ad_id_impression")

	sc.stop()

