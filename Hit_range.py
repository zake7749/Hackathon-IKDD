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
	player_name : index=19
	"""

	# player_name, Shot_distance, Shot_results
	# if missed, then it should be the breakpoint.
	if player_info[13] == "missed":
		score = -99
	else:
		score = 1

	return ((player_info[19],float(player_info[11])) ,score)

able_data = csv_rdd.map(map2Pair)
dense = able_data.reduceByKey(lambda a,b: a+b)
sorted_dense = dense.sortBy(lambda ele: ele[0][1])

ks_pair = sorted_dense.collect()

# calculate the best range on the smaller dataset.

best_range = {}
person_data = {}

for key,score in ks_pair:
	name, dist = key
	if name not in person_data.keys():
		person_data[name] = []
	person_data[name].append((dist,score))

#for k,v in person_data.items():
#	print(k,v)
	
op = open('Hit_range.txt','w')
for k,v in person_data.items():
	
	curr_st = -1
	curr_ed = -1
	curr_pt = 0
	answer_range = []

	for point,score in v:
		if score < 0:
			if curr_st == -1:
				continue
			elif curr_ed == -1:
				curr_st = -1
				curr_pt = 0
				continue
			else:
				answer_range.append((curr_st,curr_ed,curr_pt))
				curr_st = -1
				curr_ed = -1
				curr_pt = 0	
		else:
			curr_pt += score
			if curr_st == -1:
				curr_st = point
			else:
				curr_ed = point
	# special case for like (A,1),(B,2),(C,3)
	if curr_st != -1:
		answer_range.append((curr_st,curr_ed,curr_pt))
	answer_range = sorted(answer_range, key = lambda ele:ele[2] ,reverse=True)
	if len(answer_range) == 0:
		op.write("%s no\n" % k)
	else:
		op.write("%s " % k)
		former = answer_range[0][2]
		for st,ed,pt in answer_range:
			if former == pt:
				op.write("%.1f-%.1f " % (st,ed))
		op.write("\n")
op.close()


			



