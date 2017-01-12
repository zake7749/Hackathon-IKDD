from pyspark import SparkContext,SparkConf
import csv

conf = SparkConf().setAppName("Hit_Rate").setMaster("local[*]")
sc = SparkContext(conf=conf)
csv_file = open("../shot_logs.csv",'r')
csv_file.next() # Drop the header line.
data_list = csv.reader(csv_file)
csv_rdd  = sc.parallelize(data_list)

def map2Pair(info):

	"""
	Make a tuple of (GameID,Player) as key
	and the (score,win_or_lose) as Value
	"""

	game_id = info[0]
	win_lose = info[3]
	pts = int(info[18])
	name = info[19]
	
	return ((game_id,name),(win_lose,pts))

def dropGameID(ele):

	"""
	Drop the game id.
	"""

	game_id,name = ele[0]
	win_lose,pts = ele[1]

	return (name,(win_lose,pts))


def findMaxLosedPts(ele1, ele2):

	stat1,pts1 = ele1
	stat2,pts2 = ele2

	if stat1 == 'W' and stat2 == 'W':
		return ('W',0)

	else:

		if stat1 == 'W':
			return('L',pts2)
		elif stat2 == 'W':
			return('L',pts1)
		else:
			if pts1 > pts2:
				return('L',pts1)
			else:
				return('L',pts2)
	

player_gain = csv_rdd.map(map2Pair)
player_total_gain = player_gain.reduceByKey(lambda a,b: (a[0],a[1]+b[1]))

# Drop the game_ID
player_total_gain_without_ID = player_total_gain.map(dropGameID)

# Build the dict that hold the max losed score for each user.
player_with_max_lose_pts = player_total_gain_without_ID.reduceByKey(findMaxLosedPts)
max_losed_score = {}

for name,stat in player_with_max_lose_pts.collect():
	max_losed_score[name] = stat[1]

min_winned_score = {}

for name,stat in player_total_gain_without_ID.collect():
	
	status,score = stat
	if name not in min_winned_score:
		min_winned_score[name] = 9999
	if status == 'W':
		if score < min_winned_score[name] and score > max_losed_score[name]:
			min_winned_score[name] = score

with open('win_point.txt','w') as out:			
	for k,v in min_winned_score.items():
		if v == 9999:
			out.write("%s no\n" % k)
		else:
			out.write("%s %d\n" %(k,v))
		
