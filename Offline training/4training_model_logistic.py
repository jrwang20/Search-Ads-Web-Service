from __future__ import print_function
from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils

#(' 101356, 101356, 32714, 32714, 5963, 10594, 21240, 34825, 5963, 7959,1000000', 1)
if __name__ == "__main__":

    sc = SparkContext(appName="CTRLogisticRegression")

    # $example on$
    # Load and parse the data
    def parsePoint(line):
        #首先是要把括号去掉
        line = line.strip("()")
        #根据逗号进行split
        fields = line.split(',')
        #除了最后一个其他都是feature
        featurs_raw = fields[0:11] # 0 to 10 在python里面不包括11
        features = []
        #针对前面的feature再进行处理，去掉空格去掉单引号等等
        for x in featurs_raw:
            feature = float(x.strip().strip("'").strip())
            #然后就可以输入进去feature的array里面
            features.append(feature)
        #最后一个是label
        label = float(fields[11])
        #print ("label=" + str(label))
        #这个labelPoint是spark里面的一个数据结构
        #用这个数据结构，spark就可以知道，第一个是label，第二个是feature
        return LabeledPoint(label,features)

    #首先准备好SparkContext
    data = sc.textFile("./SearchAds/data/log/ctr_features_demo/part*")#这里就是把training data给读进来
    #一部分就是做training data，另一部分就是做test data
    #train是70%，test是30%，可以自己决定
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    #现在要对这个数据进行一个parse，转换成floating number，因为原来的数据是str不能用
    #要做的两件事情：1. 拿到lable； 2. 把feature数据提取出来
    #看parsePoint函数
    parsedTrainData = trainingData.map(parsePoint)
    parsedTestData = testData.map(parsePoint)
    #经过parsePoint处理以后，就得到了LabelPoint数据，有label也有feature

    # Build the model
    #现在建立一个spark里面的logistic回归模型，用上面得到的LablePoibt数据结构类型
    #这里用的是LBFGS（回顾一下spark的逻辑回归的api，上网查）
    #然后就可以train了
    model = LogisticRegressionWithLBFGS.train(parsedTrainData,intercept=False)#这个intercept的意思就是说有没有bias

    # Evaluating the model on training data
    #train结束以后，想看一下model的效果如何
    #就要用剩下的test data来验证一下
    #首先把test data里面的feature拿出来，然后输入到model里面进行一个predict
    #这个p.label就是正确的值，model.predict(p.features)就是prediction的值，这样就形成了一个实际/测试pair
    labelsAndPreds = parsedTestData.map(lambda p: (p.label, model.predict(p.features)))
    #然后根据上面生成的实际/测试的pair，来算一下误差情况
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedTestData.count())
    print("Training Error = " + str(trainErr))
    weights = model.weights
    #这些weights和bias可以打印出来，就相当于是模型训练的结果，现在是offline给训练出来了，而online的时候也会需要
    #所以要存储下来
    print("weight= ", weights)
    print("bias=",model.intercept);

    # Save and load model
    model.save(sc, "/Users/jiayangan/project/SearchAds/data/model/ctr_logistic_model_demo4")
    # $example off$