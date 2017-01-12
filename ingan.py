import csv
playerID =""
result = dict()
score=0
gameID = "init"
with open("shot_logs.csv", "r") as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=',', quotechar='\"')
        csv_reader.next()
	for row in csv_reader:
            if (row[19] not in result):
                if(gameID!= "init"):
                    if(gameID != row[0]):
                        result[playerID][status].append(score)

                status = row[3]
                gameID = row[0]
                score=0
                playerID = row[19]
                result[playerID] = dict()
                result[playerID]["W"]=[]
                result[playerID]["L"]=[]
            if(gameID != row[0]):
                result[playerID][status].append(score)
                score = 0
                playerID = row[19]
                status = row[3]
                gameID = row[0]
            score += int(row[18])
        result_list = result.items()
        with open("quiz3.txt","w") as outputFile:
            for x in result_list:
                losePoint = max(x[1]["L"])
                if(x[0] == 'kyle korver'):
                    print x
                win = -1
                minWin=111999
                for eachWin in x[1]["W"]:
                    if(eachWin > losePoint):
                        win = eachWin
                        if win < minWin:
                            minWin = win
                if( win != -1):
                    outputFile.write(str(x[0])+" "+str(minWin)+"\n")
                else:
                    outputFile.write(str(x[0])+" no\n")

