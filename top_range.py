from pyspark import SparkContext,SparkConf
import csv

conf = SparkConf().setAppName("Hit_Rate").setMaster("local[*]")
sc = SparkContext(conf=conf)
csv_file = open("../shot_logs.csv",'r')
csv_file.next() # Drop the header line.
data_list = csv.reader(csv_file)
csv_rdd  = sc.parallelize(data_list)

def map2Pair(player_info):

	"""
	Shot_distance: index=11
	Shot_results: index=13
	"""

	# player_name, Shot_distance, Shot_results
	# if missed, then it should be the breakpoint.
	score = 0

	if player_info[13] == "missed":
		score = 0
	else:
		score = 1

	# dist: {score,count}
	return (float(player_info[11]) ,(score,1))

def reduceScore(e1,e2):
	
	s1,t1 = e1
	s2,t2 = e2

	return(s1+s2,t1+t2)

def countAccuracy(v):
	key,stat = v
	s,t = stat

	# acc, total done.
	return (key ,(float(s)/float(t),s))

able_data = csv_rdd.map(map2Pair)
dense = able_data.reduceByKey(reduceScore)
#dense_with_acc = dense.map(countAccuracy)
sorted_dense = dense.sortBy(lambda ele: ele[0])
ks_pair = sorted_dense.collect()


#for ele in ks_pair:
#	print(ele)


answer = []
# calculate the best range on the smaller dataset.
# Use dynamic programming.
for i in range(0, len(ks_pair)):
	answer_list = []
	for j in range(0, len(ks_pair)):
		if j < i:
			answer_list.append((j,j,j))
		elif j == i:
			answer_list.append((ks_pair[i][1][0],ks_pair[i][1][1],float(ks_pair[i][1][0])/float(ks_pair[i][1][1])))
		else:
			f_score,f_total,f_acc = answer_list[j-1]
			new_score = ks_pair[j][1][0] + f_score
			new_total = ks_pair[j][1][1] + f_total
			answer_list.append((new_score,new_total,float(new_score)/float(new_total)))
	answer.append(answer_list)

#print(len(ks_pair)) #448
#print(len(answer)) #448
#print(len(answer[5])) #453

# Print the best range.
best_i = 0
best_j = 0
best_acc = -1

for i in range(0,len(ks_pair)):
	for j in range(0,len(ks_pair)):
		if j > i:
			score,total,acc = answer[i][j]
			if acc > best_acc:
				best_acc = acc
				best_i = i
				best_j = j

with open('top_range.txt','w') as out:
	#print(best_i)
	#print(best_j)
	out.write("%.1f-%.1f" %(ks_pair[best_i][0],ks_pair[best_j][0]))






