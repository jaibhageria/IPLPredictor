import pandas as pd
import os

df_bowl = pd.read_csv('IPL_Bowl_Cluster.csv')
df_bowl = df_bowl.drop('Unnamed: 0',1)
df_bat = pd.read_csv('IPL_Bat_Cluster.csv')
df_bat = df_bat.drop('Unnamed: 0',1)
print(df_bowl.head(1))
print(df_bat.head(1))

files = os.listdir('ipl_csv')
#files = ['1082591.csv']
files.remove('README.txt')
colnames = ['ball','innings_number','over and ball','Batting Team Name','Batsman','non-striker','Bowler','runs-off bat','extras','kind_of_wicket','dismissed_player_name']
not_wanted = ['version']
wicket_type = ['caught','bowled','run out','lbw','stumped','caught and bowled','hit wicket','obstructing the field']
#Batsman_vul_dict={}
Master_list = []
for file in files:
    #print (file)
    CurBowler = 'null'
    df = pd.read_csv('ipl_csv/'+file,names=colnames,header=None)
    df = df[~df['ball'].isin(not_wanted)]
    Inner_list = []
    HomeTeam = df.iloc[0]['over and ball']  #More than 90% of matches have first team as home team
    df = df[18:]   #First 19 rows contains 
    #print ('Home Team: ',HomeTeam)
    #print(df.head(5))
    for index,row in df.iterrows():   #For every ball
        if row['runs-off bat'] not in range(0,8):    #Accounting for any erroneous cases like D/L (1082648)
            continue
        #Over = []
        #Score = 0
        #OverBall1 = row['over and ball']  #0.1
        OverBowler = row['Bowler']
        #print ((OverBall))
        #print(index,row)
        if OverBowler != CurBowler: #first ball of new over
            #print(Inner_list)
            if (len(Inner_list)>0):
                Master_list.append (Inner_list)
            Inner_list = []
            Score = 0
            Batsman = row['Batsman'] 
            BatTeam = row['Batting Team Name']
            Non_striker = row['non-striker']
            Bowler = row['Bowler']
            Runs = int(row['runs-off bat'])
            Wicket = row['kind_of_wicket']
            O = row['over and ball']  #0.1
            OverNum = int(O.split('.')[0])
            StBat = df_bat[df_bat.Name == Batsman]
            NStBat = df_bat[df_bat.Name == Non_striker]
            CBowl = df_bowl[df_bowl.Name == Bowler]
            Inner_list.extend ([int(StBat.prediction),float(StBat.Avg),float(StBat.SR)])
            Inner_list.extend ([int(NStBat.prediction),float(NStBat.Avg),float(NStBat.SR)])
            Inner_list.extend ([int(CBowl.prediction),int(CBowl.Wickets),float(CBowl.Econ),float(CBowl.Avg)])
            if (BatTeam == HomeTeam):
                Inner_list.append(1)  #Indicates home team batsman is on strike
            else:
                Inner_list.append(0)  #Indicates away team batsman is on strike
            if (0<=OverNum<=5):   #Powerplay
                Inner_list.append(0)
            if (6<=OverNum<=14):  #Middle overs
                Inner_list.append(1)
            if (OverNum>=15):     #Death overs
                Inner_list.append(2)
            Inner_list.append(0)   #Initially, runs = 0
            Inner_list.append(0)   #Initially, wickets = 0
            if Wicket in wicket_type:
                Inner_list[13]+=1
            else:
                Inner_list[12]+=Runs
            CurBowler = OverBowler
        else:
            Runs = int(row['runs-off bat'])
            Wicket = row['kind_of_wicket']
            if Wicket in wicket_type:
                Inner_list[13]+=1
            else:
                Inner_list[12]+=Runs
    Master_list.append (Inner_list)
    #Master_list = Master_list[1:]
CSV = pd.DataFrame(Master_list)
#CSV.to_csv('IPL_over_by_over_stage_econ.csv')

from pyspark import SparkContext, SparkConf
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
sc = SparkContext('local','decisiontree')
def FeedInputDataRuns(f):
    sbat_c=int(f[0])
    sbat_a=float(f[1])
    sbat_s=float(f[2])
    nsbat_c=int(f[3])
    nsbat_a=float(f[4])
    nsbat_s=float(f[5])
    bow_c=int(f[6])
    bow_w=int(f[7])
    bow_e=float(f[8])
    bow_a=float(f[9])
    home=int(f[10])
    stage=int(f[11])  #0 = PP(First 6), 1 = Normal, 2 = DeathOvers(last 5)
    r=int(f[12])
    return LabeledPoint(r,[sbat_c,sbat_a,sbat_s,nsbat_c,nsbat_a,nsbat_s,bow_c,bow_w,bow_e,bow_a,home,stage])
def FeedInputDataWkts(f):
    sbat_c=int(f[0])
    sbat_a=float(f[1])
    sbat_s=float(f[2])
    nsbat_c=int(f[3])
    nsbat_a=float(f[4])
    nsbat_s=float(f[5])
    bow_c=int(f[6])
    bow_w=int(f[7])
    bow_e=float(f[8])
    bow_a=float(f[9])
    home=int(f[10])
    stage=int(f[11])  #0 = PP(First 6), 1 = Normal, 2 = DeathOvers(last 5)
    w=int(f[13])
    return LabeledPoint(w,[sbat_c,sbat_a,sbat_s,nsbat_c,nsbat_a,nsbat_s,bow_c,bow_w,bow_e,bow_a,home,stage])
#data = MLUtils.loadLibSVMFile(sc, 'IPL_Over.txt')
Overdata = sc.textFile("IPL_over_by_over_stage_econ.csv")
csvOverdata=Overdata.map(lambda x:x.split(","))
ipdata = csvOverdata.collect()
trainingDataRuns=csvOverdata.map(FeedInputDataRuns)
trainingDataWkts=csvOverdata.map(FeedInputDataWkts)
print(trainingDataRuns)
print(trainingDataWkts)
modelRuns = DecisionTree.trainClassifier(trainingDataRuns, numClasses=37, categoricalFeaturesInfo={0:6,3:6,6:7,10:2,11:3},
                                     impurity='gini', maxDepth=30, maxBins=32) 
modelWkts = DecisionTree.trainClassifier(trainingDataWkts, numClasses=6, categoricalFeaturesInfo={0:6,3:6,6:7,10:2,11:3},
                                     impurity='gini', maxDepth=30, maxBins=32)
#model = DecisionTree.trainRegressor(trainingData,  categoricalFeaturesInfo={0:4,3:4,6:5,9:2}, 
#                                    impurity='variance', maxDepth=30, maxBins=32) 
print(modelRuns)
print(modelWkts)
print(modelRuns.toDebugString())
print(modelWkts.toDebugString())
predictionsRuns = modelRuns.predict(trainingDataRuns.map(lambda x: x.features))
predictionsWkts = modelWkts.predict(trainingDataWkts.map(lambda x: x.features))
labelsAndPredictionsRuns = trainingDataRuns.map(lambda lp: lp.label).zip(predictionsRuns)
labelsAndPredictionsWkts = trainingDataWkts.map(lambda lp: lp.label).zip(predictionsWkts)
testErrRuns = labelsAndPredictionsRuns.filter(
    lambda lp: lp[0] == lp[1]).count() / float(trainingDataRuns.count())
print('Test Accuracy Runs= ' + str(testErrRuns))
testErrWkts = labelsAndPredictionsWkts.filter(
    lambda lp: lp[0] == lp[1]).count() / float(trainingDataWkts.count())
print('Test Accuracy Wkts= ' + str(testErrWkts))
#print('Learned classification tree model:')
#print(model.toDebugString())
resultsWkts = predictionsWkts.collect()

#for result in resultsWkts:
    #print(result)
resultsRuns = predictionsRuns.collect()

#Innings Simulation
#RCB V MI 2018
#DD V RR 2018
#Innings 1 RCB bat
#Team1 = 'DD'
#Team1 = 'RCB'
BatTeam = 'Royal Challengers Bangalore'
#BatTeam = 'Delhi Daredevils'
#HomeTeam = 'Delhi Daredevils'
HomeTeam = 'Royal Challengers Bangalore'
#HomeTeam = 'Mumbai Indians'
Team2 = 'MI'
#Team2 = 'RR'

#BatTeam = 'Kings XI Punjab'
#Team2 = 'Delhi Daredevils'
#HomeTeam = 'Delhi Daredevils'

#Batsmen = ['KL Rahul','AJ Finch','MA Agarwal','KK Nair','Yuvraj Singh','DA Miller','R Ashwin','AJ Tye','BB Sran','AS Rajpoot','Mujeeb Ur Rahman']
#Bowlers = ['TA Boult','Avesh Khan','LE Plunkett','DT Christian','A Mishra','GJ Maxwell','R Tewatia']
#Overs = ['TA Boult','Avesh Khan','TA Boult','Avesh Khan','LE Plunkett','Avesh Khan','DT Christian','LE Plunkett','DT Christian','A Mishra','GJ Maxwell','A Mishra','Avesh Khan','A Mishra','R Tewatia','A Mishra','LE Plunkett','DT Christian','LE Plunkett','TA Boult']

Batsmen = ['M Vohra','Q de Kock','BB McCullum','V Kohli','AB de Villiers','C de Grandhomme','Washington Sundar','TG Southee','UT Yadav','YS Chahal','Mohammed Siraj']
#Batsmen = ['PP Shaw','C Munro','SS Iyer','RR Pant','GJ Maxwell','V Shankar','LE Plunkett','A Mishra','S Nadeem','Avesh Khan','TA Boult']
Bowlers = ['JP Duminy','MJ McClenaghan','JJ Bumrah','KH Pandya','HH Pandya','M Markande']
#Bowlers = ['DS Kulkarni','JC Archer','K Gowtham','JD Unadkat','S Gopal','BA Stokes']
Overs = ['JP Duminy','MJ McClenaghan','JJ Bumrah','JP Duminy','MJ McClenaghan','KH Pandya','M Markande','KH Pandya','M Markande','HH Pandya','KH Pandya','M Markande','KH Pandya','JJ Bumrah','MJ McClenaghan','HH Pandya','JJ Bumrah','HH Pandya','JJ Bumrah','MJ McClenaghan']
#Overs = ['DS Kulkarni','JC Archer','DS Kulkarni','K Gowtham','JD Unadkat','S Gopal','BA Stokes','S Gopal','BA Stokes','JC Archer','JD Unadkat','K Gowtham','DS Kulkarni','BA Stokes','JD Unadkat','JC Archer','JD Unadkat']
#Health = {name:1 for name in Batsmen}
batcnt = 0
Runs = 0
PrevWkt = 0
Wkt = 0
Batsman = Batsmen[batcnt]
batcnt+=1
Non_striker = Batsmen[batcnt]
#Batsmen_scores = {name:[0,0] for name in Batsmen}  #Runs, balls faced
#Bowler_wickets = {name:[0,0,0] for name in Bowlers}  #Wickets, Runs given
for X in range(len(Overs)):
    print ("Over:" + str(X+1) + '\n')
    Bowler = Overs[X]
    StBat = df_bat[df_bat.Name == Batsman]
    NStBat = df_bat[df_bat.Name == Non_striker]
    CBowl = df_bowl[df_bowl.Name == Bowler]
    Inner_list = []
    Inner_list.extend ([int(StBat.prediction),float(StBat.Avg),float(StBat.SR)])
    Inner_list.extend ([int(NStBat.prediction),float(NStBat.Avg),float(NStBat.SR)])
    Inner_list.extend ([int(CBowl.prediction),int(CBowl.Wickets),float(CBowl.Econ),float(CBowl.Avg)])
    if (BatTeam == HomeTeam):
        Inner_list.append(1)  #Indicates home team batsman is on strike
    else:
        Inner_list.append(0)  #Indicates away team batsman is on strike
    if (X < 6):  #Powerplay
        Inner_list.append(0)
    elif (X>len(Overs)-6):     #Death overs
        Inner_list.append(2)
    else:
        Inner_list.append(1)
    #Inner_list.append(2)
    Test_Array  = [np.array(Inner_list)]
    #testData = sc.parallelize(Test_Array)
    testData = sc.parallelize(Test_Array)
    predictRuns = modelRuns.predict(testData)
    predictWkts = modelWkts.predict(testData)
    resultRuns = predictRuns.collect()
    resultWkts = predictWkts.collect()
    Runs+=int(resultRuns[0])
    Temp = int(resultWkts[0])
    Wkt+=Temp
    
    print ("Score at end of over is: " + str(Runs) + " / " + str(min(Wkt,10)))
    if (Wkt>=10):
        break
    if (Temp>0):
        batcnt+=int(Temp)
        Batsman = Batsmen[batcnt]
    Batsman,Non_striker = Non_striker,Batsman
print ("Team score at end of innings: " + str(Runs) + " / " + str(min(Wkt,10)))
Target = Runs+1
print ("Target for " + Team2 + " is "+ str(Target))
#Innings Simulation
#RCB V MI 2018
#DD V RR 2018
#Innings 2 MI bat
#Team1 = 'DD'
#Team1 = 'RCB'

#BatTeam = 'Delhi Daredevils'
#HomeTeam = 'Kings XI Punjab'

#Batsmen = ['PP Shaw','G Gambhir','GJ Maxwell','SS Iyer','RR Pant','DT Christian','R Tewatia','LE Plunkett', 'A Mishra','Avesh Khan','TA Boult']
#Bowlers = ['AS Rajpoot','BB Sran','AJ Tye','R Ashwin','Mujeeb Ur Rahman']
#Overs = ['AS Rajpoot','BB Sran','AS Rajpoot','AJ Tye','AS Rajpoot','AJ Tye','AS Rajpoot','R Ashwin','Mujeeb Ur Rahman','R Ashwin','Mujeeb Ur Rahman','R Ashwin','BB Sran','R Ashwin','Mujeeb Ur Rahman','AJ Tye','BB Sran','AJ Tye','BB Sran','Mujeeb Ur Rahman']

BatTeam = 'Mumbai Indians'
#BatTeam = 'Rajasthan Royals'
#BatTeam = 'Delhi Daredevils'
#HomeTeam = 'Delhi Daredevils'
#HomeTeam = 'Rajasthan Royals'
Team2 = 'RCB'
Team2 = 'DD'
HomeTeam = 'Royal Challengers Bangalore'
#HomeTeam = 'Mumbai Indians'
#Team2 = 'MI'
#Innings 2 MI bat
Batsmen = ['SA Yadav','Ishan Kishan','JP Duminy','RG Sharma','KA Pollard','KH Pandya','HH Pandya','BCJ Cutting','MJ McClenaghan','M Markande','JJ Bumrah']
#Batsmen = ['DJM Short','JC Buttler','SV Samson','BA Stokes','RA Tripathi','K Gowtham','JC Archer','AM Rahane','JD Unadkat','DS Kulkarni','S Gopal']
Bowlers = ['TG Southee','UT Yadav','Mohammed Siraj','YS Chahal','Washington Sundar','C de Grandhomme']
#Bowlers = ['S Nadeem','TA Boult','Avesh Khan','LE Plunkett','A Mishra','GJ Maxwell']
Overs = ['TG Southee','UT Yadav','TG Southee','UT Yadav','Mohammed Siraj','UT Yadav','YS Chahal','Mohammed Siraj','YS Chahal','Washington Sundar','YS Chahal','C de Grandhomme','YS Chahal','C de Grandhomme','UT Yadav','C de Grandhomme','Mohammed Siraj','TG Southee','Mohammed Siraj','TG Southee']
#Overs = ['S Nadeem','TA Boult','Avesh Khan','LE Plunkett','A Mishra','Avesh Khan','A Mishra','LE Plunkett','TA Boult','GJ Maxwell','LE Plunkett','TA Boult']
#Health = {name:1 for name in Batsmen}
batcnt = 0
Runs = 0
#PrevWkt = 0
Wkt = 0
Batsman = Batsmen[batcnt]
batcnt+=1
Non_striker = Batsmen[batcnt]
#Batsmen_scores = {name:[0,0] for name in Batsmen}  #Runs, balls faced
#Bowler_wickets = {name:[0,0,0] for name in Bowlers}  #Wickets, Runs given
for X in range(len(Overs)):
    print ("Over:" + str(X+1) + '\n')
    Bowler = Overs[X]
    StBat = df_bat[df_bat.Name == Batsman]
    NStBat = df_bat[df_bat.Name == Non_striker]
    CBowl = df_bowl[df_bowl.Name == Bowler]
    Inner_list = []
    Inner_list.extend ([int(StBat.prediction),float(StBat.Avg),float(StBat.SR)])
    Inner_list.extend ([int(NStBat.prediction),float(NStBat.Avg),float(NStBat.SR)])
    Inner_list.extend ([int(CBowl.prediction),int(CBowl.Wickets),float(CBowl.Econ),float(CBowl.Avg)])
    if (BatTeam == HomeTeam):
        Inner_list.append(1)  #Indicates home team batsman is on strike
    else:
        Inner_list.append(0)  #Indicates away team batsman is on strike
    if (X < 6):  #Powerplay
        Inner_list.append(0)
    elif (X>len(Overs)-6):     #Death overs
        Inner_list.append(2)
    else:
        Inner_list.append(1)
    Test_Array  = [np.array(Inner_list)]
    testData = sc.parallelize(Test_Array)
    predictRuns = modelRuns.predict(testData)
    predictWkts = modelWkts.predict(testData)
    resultRuns = predictRuns.collect()
    resultWkts = predictWkts.collect()
    Runs+=int(resultRuns[0])
    Temp = int(resultWkts[0])
    Wkt+=Temp
    
    print ("Score at end of over is: " + str(Runs) + " / " + str(Wkt))
    if (Wkt==10 or Runs>=Target):
        break
    if (Temp>0):
        batcnt+=int(Temp)
        Batsman = Batsmen[batcnt]
    Batsman,Non_striker = Non_striker,Batsman
print ("Team score at end of innings: " + str(Runs) + " / " + str(Wkt))

