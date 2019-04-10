import os
import sys
import glob
import libmc
from libmc import (
    MC_HASH_MD5, MC_POLL_TIMEOUT, MC_CONNECT_TIMEOUT, MC_RETRY_TIMEOUT
)

# Gnerate training files - Process log file and select features from cachee

from pyspark import SparkContext
def process_query(query):
    fields = query.split(" ")
    output = "_".join(fields)
    return output

# log: Device IP, Device id,Session id,Query,AdId,CampaignId,Ad_category_Query_category(0/1),clicked(0/1)
def prepare_feature_val(fields,memcache_client):
    device_ip = fields[0]
    device_id = fields[1]
    query = process_query(fields[3])
    ad_id = fields[4]
    camp_id = fields[5]
    query_ad_category_match = fields[6]

    #首先construct所有的key

    #如果match就是1000000，否则就是0，不需要去key value store里面做look up
    #因为只需要知道他们是否match就可以了，不需要知道具体是哪两个
    #如果match的话，click的可能性就高
    #为什么要把1换成1000000呢？因为如果用1的话，这个feature和其他的value比起来不在一个级别上
    #machine learning里面的feature scale，这个feature的数量级要达到一定程度，才能对最终结果有影响
    if query_ad_category_match == '1':
        query_ad_category_match = '1000000'
    else:
        query_ad_category_match = '0'

    #对于后面的feature，首先构造一个key，构造的这个key要和存储时的key相同，也就是store_feature时的一样，从而能够从memcache里面拿出来value
    #value就是那些click值和impression的值
    #这里面和store_feature程序联系很密切，要连起来看
    device_ip_click_key = "dipc_" + device_ip
    device_ip_click_val = memcache_client.get(device_ip_click_key)
    print "key=",device_ip_click_key
    print "val=",device_ip_click_val
    #如果没有的话就是0
    if not device_ip_click_val:
        device_ip_click_val = "0"

    #后面都一样，根据key把memcache里面的value取出来
    device_ip_impression_key = "dipi_" + device_ip
    device_ip_impression_val = memcache_client.get(device_ip_impression_key)
    print device_ip_impression_val
    if not device_ip_impression_val:
            device_ip_impression_val = "0"

    device_id_click_key = "didc_" + device_id
    device_id_click_val = memcache_client.get(device_id_click_key)
    if not device_id_click_val:
        device_id_click_val = "0"

    device_id_impression_key = "didi_" + device_id
    device_id_impression_val = memcache_client.get(device_id_impression_key)
    if not device_id_impression_val:
        device_id_impression_val = "0"

    ad_id_click_key = "aidc_" + ad_id
    ad_id_click_val = memcache_client.get(ad_id_click_key)
    if not ad_id_click_val:
        ad_id_click_val = "0"

    ad_id_impression_key = "aidi_" + ad_id
    ad_id_impression_val = memcache_client.get(ad_id_impression_key)
    if not ad_id_impression_val:
        ad_id_impression_val = "0"

    query_campaign_id_click_key = "qcidc_" + query + "_" + camp_id;
    query_campaign_id_click_val = memcache_client.get(query_campaign_id_click_key)
    if not query_campaign_id_click_val:
        query_campaign_id_click_val = "0"

    query_campaign_id_impression_key = "qcidi_" + query + "_" + camp_id;
    query_campaign_id_impression_val = memcache_client.get(query_campaign_id_impression_key)
    if not query_campaign_id_impression_val:
        query_campaign_id_impression_val = "0"

    query_ad_id_click_key = "qaidc_" + query + "_" + ad_id;
    query_ad_id_click_val = memcache_client.get(query_ad_id_click_key)
    if not query_ad_id_click_val:
        query_ad_id_click_val = "0"

    query_ad_id_impression_key = "qaidi_" + query + "_" + ad_id;
    query_ad_id_impression_val = memcache_client.get(query_ad_id_impression_key)
    if not query_ad_id_impression_val:
        query_ad_id_impression_val = "0"

    #等到都取出来以后，就可以构建vector了，这个vector也就是training data
    #这里是在准备offline的training data
    #等到online的时候，顺序还是要和这里的顺序一样
    features = []
    features.append(str(device_ip_click_val))
    features.append(str(device_ip_impression_val))
    features.append(str(device_id_click_val))
    features.append(str(device_id_impression_val))
    features.append(str(ad_id_click_val))
    features.append(str(ad_id_impression_val))
    features.append(str(query_campaign_id_click_val))
    features.append(str(query_campaign_id_impression_val))
    features.append(str(query_ad_id_click_val))
    features.append(str(query_ad_id_impression_val))
    features.append(query_ad_category_match)

    line = ",".join(features)
    #print line
    return line

if __name__ == "__main__":
    #首先input
    file = sys.argv[1] #log file
    #这个client就是memcache的对象
    client = memcache.Client([('127.0.0.1', 11218)])
    # client = libmc.Client(
    #     ["127.0.0.1:11218"],comp_threshold=0, noreply=False, prefix=None,hash_fn=MC_HASH_MD5, failover=False
    # )
    #然后创建一个spark context
    sc = SparkContext(appName="CTR_Features")
    output_dir = 'spark/cs209'
    #然后生成一个RDD，针对每一个line进行split
    data = sc.textFile(file).map(lambda line: line.encode("utf8", "ignore").split(','))
    #现在只做一个map，不用做reduce
    #因为只是要用spark这个平台帮我们切割这个大的文件就好了
    #针对map的输出，一个是lable，另一个是feature value本身
    #这里还传了一个memcache的client
    #现在去看prepare_feature_val函数
    feature_data = data.map(lambda fields: (prepare_feature_val(fields, client),int(fields[7])))

    #现在准备好了，就save起来
    feature_data.saveAsTextFile(output_dir + "ctr_features_demo3")
    sc.stop()