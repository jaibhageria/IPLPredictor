from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import numpy as np
import matplotlib.pyplot as plt
from numpy import array
from math import sqrt
#%matplotlib inline

spark = SparkSession.builder.appName('ipl_bat_cluster').getOrCreate()
data = spark.read.csv('bat_profile_ipl.csv', header=True, inferSchema=True)
#data = spark.read.csv('hdfs://master:9000/inputdata/bat_profile_ipl.csv')
data.printSchema()
data.show(20)
print(type(data))
data2 = data.select('PID','Name','Runs','Avg','SR')
data2.show()
cols = ['Runs','Avg','SR']  #cluster parameters
assembler = VectorAssembler(inputCols=cols, outputCol='features')
assembled_data = assembler.transform(data2)  #Now, assembled_data is a pyspark sql dataframe
assembled_data.show(20)
#Computing cost to determine most efficient k
cost = np.zeros(20)
for k in range(2,20):
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(assembled_data.sample(False,0.1, seed=42))
    cost[k] = model.computeCost(assembled_data)
fig, ax = plt.subplots(1,1, figsize =(8,6))
ax.plot(range(2,20),cost[2:20])
ax.set_xlabel('k')
ax.set_ylabel('cost')

#From the plot, we can see that optimal k is 6. In this example, we will consider 6
k_means_bat = KMeans(featuresCol='features', k=6)
model_bat = k_means_bat.fit(assembled_data)
KMeans_bat_fitted = model_bat.transform(assembled_data)
KMeans_bat_fitted.show()
KMeans_bat_fitted.groupBy('prediction').count().show()
KMeans_bat_fitted.filter(KMeans_bat_fitted.prediction==1).show(60)
#KMeans_bat_fitted.toPandas().to_csv('IPL_Bat_Cluster.csv')