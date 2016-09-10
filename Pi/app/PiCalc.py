import pyspark
from pyspark import SparkContext, SparkConf
import numpy as np
import time

#=============SETUP SPARK==================
local = False

if local:
    spark = pyspark.SparkContext("local[*]")
    spark.setLogLevel("ALL")

else:
    import os
    master = os.environ["SPARK_MASTER"]
    master = "spark://{}:7077".format(master)
    conf = SparkConf().setAppName("SpotTrawl").setMaster(master)
    spark = SparkContext(conf=conf)

#===========DEFINE SAMPLING FUNCTION=======
numSamples = 10**7
def sample(p):
    x, y = np.random.random(), np.random.random()
    return 1 if x*x + y*y < 1 else 0


#==========TAKE SAMPLES======================
count = spark.parallelize(xrange(0, numSamples)).map(sample) \
             .reduce(lambda a, b: a + b)

#==========ESTIMATE 4pi======================
piEst = 4.0 * count / numSamples

#==========FIND NUMBER OF MATCHING DIGITS====
n, pi = 0, np.pi
while int(pi*10**n) == int(piEst*10**n):
	n+=1

#=========PRINT RESULTS FOR OBSERVATION======
print "Pi is roughly {}".format(piEst)
print "Error: {}%".format((piEst-pi)/pi)
print "Matching Digits: {}".format(n)
print("DESIRED OUTPUT LENGTH: 3")
time.sleep(1000)

