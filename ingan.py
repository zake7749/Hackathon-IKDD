import csv
gameID = "init"
playerID =""
result = dict()
score=0
ssss = []
with open("shot_logs.csv", "r") as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=',', quotechar='\"')
        csv_reader.next()
	for row in csv_reader:
            playerID = row[19]
            status = row[3]
            if(gameID != row[0]):
                gameID = row[0]
                if(playerID not in result):
                    result[playerID]=dict()
                    result[playerID]["W"]=[]
                    result[playerID]["L"]=[]
                result[playerID][status].append(score)
                score = 0
            else :
                if (row[19] not in result):
                    diction[row[19]]= []
                else:
                    score += int(row[18])
        result_list = result.items()
        with open("quiz3.txt","w") as outputFile:
            for x in result_list:
                losePoint = max(x[1]["L"])
                win = -1
                for eachWin in x[1]["W"]:
                    if(eachWin > losePoint):
                        win = eachWin
                if(win != -1):
                    outputFile.write(str(x[0])+" "+str(win)+"\n")
                else:
                    outputFile.write(str(x[0])+" no\n")

