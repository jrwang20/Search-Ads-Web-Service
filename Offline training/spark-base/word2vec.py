import sys
import time
import json
from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

training_file = sys.argv[1]
synonyms_data_file = sys.argv[2]

sc = SparkContext(appName="word2vec")
#首先生成了一个RDD
inp = sc.textFile(training_file).map(lambda line: line.encode("utf8", "ignore").split(" "))

#创建一个word2vec的model
word2vec = Word2Vec()
#millis = int(round(time.time() * 1000))
#model = word2vec.setMinCount(5).setVectorSize(10).setSeed(2017).fit(inp)
#model = word2vec.setVectorSize(10).setSeed(2017).fit(inp)
#首先设置model的超参数，然后用上面的RDD训练这个model
model = word2vec.setLearningRate(0.02).setMinCount(5).setVectorSize(10).setSeed(2017).fit(inp)

#model = word2vec.setMinCount(5).setVectorSize(10).setSeed(2017).fit(inp)

#training完了以后得到了一个vector
#和上面的vector不一样，这个vector是training数据里所有的word对应的最终的vector
vec = model.getVectors()
synonyms_data = open(synonyms_data_file, "w")

print "len of vec", len(vec)
#要把每一个word对应的近义词存下来
for word in vec.keys():
    #用这个findSynonyms方法，找出word的5个synonyms
    synonyms = model.findSynonyms(word, 5)
    #建立一个dictionary用来store结果
    entry = {}
    entry["word"] = word
    synon_list = []
    #根据cosine来表示，计算夹角，计算排名
    for synonym, cosine_distance in synonyms:
        synon_list.append(synonym)
    entry["synonyms"] = synon_list
    #最终存到文件里
    synonyms_data.write(json.dumps(entry))
    synonyms_data.write('\n')

synonyms_data.close()

#print vec
test_data = ["dslr", "furniture", "shaver", "toddler", "sport","powershot", "xbox", "led"]
for w in test_data:
    synonyms = model.findSynonyms(w, 5)
    print "synonyms of ",w
    for word, cosine_distance in synonyms:
        print("{}: {}".format(word, cosine_distance))
#储存训练好的model
model.save(sc, "../data/model/word2vec_new2")
sc.stop()
