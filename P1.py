from pyspark import SparkContext,SparkConf
import csv

conf = SparkConf().setAppName("Hit_Rate").setMaster("local[*]")
sc = SparkContext(conf=conf)
csv_file = open("../shot_logs.csv",'r')
csv_file.next() # Drop the header line.
data_list = csv.reader(csv_file)
csv_rdd  = sc.parallelize(data_list)

def map2pair(player_info):
        # missed or not : index-13
        # player name : index-19
        total = 1
        done  = 0

        if player_info[13] != "missed":
                done = 1

        return (player_info[19],(done,total))

def scoreReduce(p1,p2):
        p1_made,p1_t = p1
        p2_made,p2_t = p2
        return (p1_made+p2_made,p1_t+p2_t)

def getAccuracy(player_info):

        name, count = player_info
        made, total = count
        acc = float(made)/float(total)
        return (name, acc)


player_single_score_pair = csv_rdd.map(map2pair)
player_total_score_pair = player_single_score_pair.reduceByKey(scoreReduce)
player_accuracy = player_total_score_pair.map(getAccuracy)
player_accuracy_sorted = player_accuracy.sortBy(lambda ele: ele[1])

results = player_accuracy_sorted.collect()

with open("Hit_Rate.txt","w") as out:
        for name,acc in results[1:]:
                out.write("%s %.2f\n" % (name,acc))
