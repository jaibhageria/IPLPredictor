import pandas as pd
import os


files = os.listdir('ipl_csv')
files.remove('README.txt')
colnames = ['ball','innings_number','over and ball','Batting Team Name','Batsman','non-striker','Bowler','runs-off bat','extras','kind_of_wicket','dismissed_player_name']
not_wanted = ['version','info']
wicket_type = ['caught','bowled','lbw','stumped','caught and bowled']
Batsman_vul_dict={}
for file in files:
    df = pd.read_csv('ipl_csv/'+file,names=colnames,header=None)
    df = df[~df['ball'].isin(not_wanted)]
    for index,row in df.iterrows():
        Batsman = row['Batsman'] 
        Bowler = row['Bowler']
        Runs = int(row['runs-off bat'])
        Wicket = row['kind_of_wicket']
        wicket = 0
        ball = 1
        if Wicket in wicket_type:
            wicket = 1
            ball = 0
        else:
            wicket = 0
            ball = 1
        if Batsman in Batsman_vul_dict:
            if Bowler in Batsman_vul_dict[Batsman]:
                Batsman_vul_dict[Batsman][Bowler][Runs]+=ball
                Batsman_vul_dict[Batsman][Bowler][7]+=wicket
            else:
                Batsman_vul_dict[Batsman][Bowler]=[0,0,0,0,0,0,0,0]    #[0,1,2,3,4,5,6,Wicket]
                Batsman_vul_dict[Batsman][Bowler][Runs]+=ball
                Batsman_vul_dict[Batsman][Bowler][7]+=wicket
        else:
            Batsman_vul_dict[Batsman]={}
            Batsman_vul_dict[Batsman][Bowler]=[0,0,0,0,0,0,0,0]    #[0,1,2,3,4,5,6,Wicket]
            Batsman_vul_dict[Batsman][Bowler][Runs]+=ball
            Batsman_vul_dict[Batsman][Bowler][7]+=wicket

Batsmen_name_list = list(Batsman_vul_dict.keys())
for X in range(len(Batsmen_name_list)):
    Bowler_name_list = list(Batsman_vul_dict[Batsmen_name_list[X]].keys())
    #TT = Batsmen_name_list[X][Batsmen]
    Combos = list(Batsman_vul_dict[Batsmen_name_list[X]].values())
    for Y in range(len(Combos)):
        L = Combos[Y]  #L is a list with  8 dimensions, analogous to Temp_list defined above
        if (sum(L)<6): #threshold = 6, less than 6 deliveries head to head is not considered
            Batsman_vul_dict[Batsmen_name_list[X]][Bowler_name_list[Y]] = [-1,-1]
        else:
            Some = []
            for Z in range(len(L)-1):
                if (sum(L[:-1])!=0):
                    Some.append (float(sum(L[:Z+1])) / float(sum(L[:-1])))
                else:
                    Some.append (0)   #need to check on this
            if (sum(L)!=0):
                Some.append (1-(float(L[7]) / float(sum(L))))  #Instead of wickets, have probability of remaining not out 
            else:
                Some.append (1)
            #Combos[Y] = Some
            Batsman_vul_dict[Batsmen_name_list[X]][Bowler_name_list[Y]] = Some
        
#print(A)
#Batsman_vul_dict[A[0]].values()
df = pd.read_csv('IPL_Bowl_Cluster.csv')
df = df.drop('Unnamed: 0',1)
df2 = pd.read_csv('IPL_Bat_Cluster.csv')
df2 = df2.drop('Unnamed: 0',1)
def getProb(Strike_bat,Cur_Bowler):
    
    #First try Bowler's Cluster
    player = Cur_Bowler
    player_row = df.loc[df['Name'] == player]
    his_cluster = df.loc[df['prediction']==int(player_row.prediction)]
    cluster_names = list(his_cluster.Name)
    cnt = 0
    Some = [0,0,0,0,0,0,0,0]
    L = [-1,-1]
    for name in cluster_names:
        if Strike_bat in Batsman_vul_dict:
            if name in Batsman_vul_dict[Strike_bat]:
                L = Batsman_vul_dict[Strike_bat][name]
            else:
                L = [-1,-1]
        else:
            L = [-1,-1]
        if L[0]==-1:
            cnt+=1
        else:
            Some = [sum(x) for x in zip(Some, L)]
    tot = len(cluster_names)-cnt
    if tot>0:
        Some = [float(x)/float(tot) for x in Some]
        return Some
    
    #Now tot is=0 and that means all bowlers failed threshold, Now we look at all Batsmen against Current Bowler
    player = Strike_bat
    player_row = df2.loc[df2['Name'] == player]
    his_cluster = df2.loc[df2['prediction']==int(player_row.prediction)]
    cluster_names = list(his_cluster.Name)
    cnt = 0
    Some = [0,0,0,0,0,0,0,0]
    L = [-1,-1]
    for name in cluster_names:
        if name in Batsman_vul_dict:
            if Cur_Bowler in Batsman_vul_dict[name]:
                L = Batsman_vul_dict[name][Cur_Bowler]
            else:
                L = [-1,-1]
        else:
            L = [-1,-1]
        if L[0]==-1:
            cnt+=1
        else:
            Some = [sum(x) for x in zip(Some, L)]
    tot = len(cluster_names)-cnt
    if tot>0:
        Some = [float(x)/float(tot) for x in Some]
        return Some
    
    #Now since both the cases did not work we take all batsmen from Strike_bat cluster and all bowlers from Cur_Bowler cluster and check all possible combinations
    player1 = Strike_bat
    player2 = Cur_Bowler
    player_row = df.loc[df['Name'] == player2]
    his_cluster = df.loc[df['prediction']==int(player_row.prediction)]
    cluster_names_2 = list(his_cluster.Name)
    player_row = df2.loc[df2['Name'] == player1]
    his_cluster = df2.loc[df2['prediction']==int(player_row.prediction)]
    cluster_names_1 = list(his_cluster.Name)
    cnt = 0
    Some = [0,0,0,0,0,0,0,0]
    L = [-1,-1]
    for name1 in cluster_names_1:
        for name2 in cluster_names_2:
            if name1 in Batsman_vul_dict:
                if name2 in Batsman_vul_dict[name1]:
                    L = Batsman_vul_dict[name1][name2]
                else:
                    L = [-1,-1]
            else:
                L = [-1,-1]
            if L[0]==-1:
                cnt+=1
            else:
                Some = [sum(x) for x in zip(Some, L)]
    tot = (len(cluster_names_1)*len(cluster_names_2))-cnt
    if tot>0:
        Some = [float(x)/float(tot) for x in Some]
        return Some
#Innings Simulation
#RCB V MI 2018
#Innings 1 RCB bat
#Team1 = 'DD'
#Team2 = 'RR'
#BatTeam = 'Kings XI Punjab'
BatTeam = 'Royal Challengers Bangalore'

#Team2 = 'Delhi Daredevils'
Team2 = 'Mumbai Indians'
HomeTeam = BatTeam

#Batsmen = ['KL Rahul','AJ Finch','MA Agarwal','KK Nair','Yuvraj Singh','DA Miller','R Ashwin','AJ Tye','BB Sran','AS Rajpoot','Mujeeb Ur Rahman']
#Bowlers = ['TA Boult','Avesh Khan','LE Plunkett','DT Christian','A Mishra','GJ Maxwell','R Tewatia']
#Overs = ['TA Boult','Avesh Khan','TA Boult','Avesh Khan','LE Plunkett','Avesh Khan','DT Christian','LE Plunkett','DT Christian','A Mishra','GJ Maxwell','A Mishra','Avesh Khan','A Mishra','R Tewatia','A Mishra','LE Plunkett','DT Christian','LE Plunkett','TA Boult']

Batsmen = ['M Vohra','Q de Kock','BB McCullum','V Kohli','AB de Villiers','C de Grandhomme','Washington Sundar','TG Southee','UT Yadav','YS Chahal','Mohammed Siraj']
#Batsmen = ['PP Shaw','C Munro','SS Iyer','RR Pant','GJ Maxwell','V Shankar','LE Plunkett','A Mishra','S Nadeem','Avesh Khan','TA Boult']
Bowlers = ['JP Duminy','MJ McClenaghan','JJ Bumrah','KH Pandya','HH Pandya','M Markande']
#Bowlers = ['DS Kulkarni','JC Archer','K Gowtham','JD Unadkat','S Gopal','BA Stokes']
Overs = ['JP Duminy','MJ McClenaghan','JJ Bumrah','JP Duminy','MJ McClenaghan','KH Pandya','M Markande','KH Pandya','M Markande','HH Pandya','KH Pandya','M Markande','KH Pandya','JJ Bumrah','MJ McClenaghan','HH Pandya','JJ Bumrah','HH Pandya','JJ Bumrah','MJ McClenaghan']
#Overs = ['DS Kulkarni','JC Archer','DS Kulkarni','K Gowtham','JD Unadkat','S Gopal','BA Stokes','S Gopal','BA Stokes','JC Archer','JD Unadkat','K Gowtham','DS Kulkarni','BA Stokes','JD Unadkat','JC Archer','JD Unadkat']
Health = {name:1 for name in Batsmen}
batcnt = 0
Runs = 0
Wkt = 0
Strike_bat = Batsmen[batcnt]
batcnt+=1
Off_Striker = Batsmen[batcnt]
Batsmen_scores = {name:[0,0] for name in Batsmen}  #Runs, balls faced
Bowler_wickets = {name:[0,0,0] for name in Bowlers}  #Wickets, Runs given
for X in range(len(Overs)):
    print ("Over:" + str(X+1) + '\n')
    Cur_Bowler = Overs[X]
    Prob = [-1,-1]
    if Strike_bat in Batsman_vul_dict:
        if Cur_Bowler in Batsman_vul_dict[Strike_bat]:
            Prob = (Batsman_vul_dict[Strike_bat][Cur_Bowler])
        else:
            Prob = [-1,-1]
    else:
        Batsman_vul_dict[Strike_bat] = {}
        Prob = [-1,-1]
    if Prob[0]==-1:
        Prob = getProb(Strike_bat,Cur_Bowler)
        Batsman_vul_dict[Strike_bat][Cur_Bowler] = Prob
    for ball in range(6):    #6 balls in one over
        R_num = random()
        init = 0
        Health[Strike_bat] *= Prob[7]
        if (Health[Strike_bat] >=0.5):
            for it in range(7):
                if (init <= R_num < Prob[it]):
                    Runs += it
                    Batsmen_scores[Strike_bat][0]+=it
                    Batsmen_scores[Strike_bat][1]+=1
                    Bowler_wickets[Cur_Bowler][1]+=it  #Runs added to bowler
                    Bowler_wickets[Cur_Bowler][2]+=1   #Add a ball bowled by bowler
                    print (str(X) + '.' + str(ball+1) + " : " + Cur_Bowler + " to " + Strike_bat + ", " + str(it) + " runs.")
                    if(it==1 or it==3 or it==5):
                        Strike_bat,Off_Striker=Off_Striker,Strike_bat
                        if Cur_Bowler in Batsman_vul_dict[Strike_bat]:
                            Prob = (Batsman_vul_dict[Strike_bat][Cur_Bowler])
                        else:
                            Prob = [-1,-1]
                        if Prob[0]==-1:
                            Prob = getProb(Strike_bat,Cur_Bowler)
                            Batsman_vul_dict[Strike_bat][Cur_Bowler] = Prob
                    break
                else:
                    init = Prob[it]
        else:
            Batsmen_scores[Strike_bat][1]+=1  #Incrementing ball faced by batsman
            Bowler_wickets[Cur_Bowler][2]+=1  #Incrementing ball of bowler
            print (str(X) + '.' + str(ball+1) + " : " + Cur_Bowler+" to "+Strike_bat+", OUT....."+Strike_bat+" made "+str(Batsmen_scores[Strike_bat][0])+" runs off " + str(Batsmen_scores[Strike_bat][1]) + " balls.")
            batcnt += 1
            Strike_bat = Batsmen[batcnt]
            if Strike_bat in Batsman_vul_dict:
                if Cur_Bowler in Batsman_vul_dict[Strike_bat]:
                    Prob = (Batsman_vul_dict[Strike_bat][Cur_Bowler])
                else:
                    Prob = [-1,-1]
            else:
                Prob = getProb(Strike_bat, Cur_Bowler)
            if Prob[0]==-1:
                Prob = getProb(Strike_bat,Cur_Bowler)
                Batsman_vul_dict[Strike_bat][Cur_Bowler] = Prob
            Wkt+=1
            Bowler_wickets[Cur_Bowler][0]+=1
        if (Wkt==10):
            break
    print ("Score at end of over " + str(X+1) + " is: " + str(Runs) + "/" + str(Wkt) + '\n')
    if (Wkt==10):
        break
    Strike_bat,Off_Striker=Off_Striker,Strike_bat
#print (Team1 + " Batting card")
print (Batsmen_scores)
#print (Team2 + " Bowling card")
for B in list(Bowler_wickets.keys()):
    Bowler_wickets[B][0],Bowler_wickets[B][1],Bowler_wickets[B][2] = Bowler_wickets[B][2]//6 + (Bowler_wickets[B][2]%6)/10, Bowler_wickets[B][1],Bowler_wickets[B][0] 
print (Bowler_wickets)
Target = Runs+1
#print ("Target for " + Team2 + " is "+ str(Target))
#Innings Simulation
#RCB V MI 2018
#Innings 2 MI bat
#BatTeam = 'Delhi Daredevils'
#HomeTeam = 'Kings XI Punjab'

BatTeam = 'Mumbai Indians'
HomeTeam = 'Royal Challengers Bangalore'

#Batsmen = ['PP Shaw','G Gambhir','GJ Maxwell','SS Iyer','RR Pant','DT Christian','R Tewatia','LE Plunkett', 'A Mishra','Avesh Khan','TA Boult']
#Bowlers = ['AS Rajpoot','BB Sran','AJ Tye','R Ashwin','Mujeeb Ur Rahman']
#Overs = ['AS Rajpoot','BB Sran','AS Rajpoot','AJ Tye','AS Rajpoot','AJ Tye','AS Rajpoot','R Ashwin','Mujeeb Ur Rahman','R Ashwin','Mujeeb Ur Rahman','R Ashwin','BB Sran','R Ashwin','Mujeeb Ur Rahman','AJ Tye','BB Sran','AJ Tye','BB Sran','Mujeeb Ur Rahman']

Batsmen = ['SA Yadav','Ishan Kishan','JP Duminy','RG Sharma','KA Pollard','KH Pandya','HH Pandya','BCJ Cutting','MJ McClenaghan','M Markande','JJ Bumrah']
#Batsmen = ['DJM Short','JC Buttler','SV Samson','BA Stokes','RA Tripathi','K Gowtham','JC Archer','AM Rahane','JD Unadkat','DS Kulkarni','S Gopal']
Bowlers = ['TG Southee','UT Yadav','Mohammed Siraj','YS Chahal','Washington Sundar','C de Grandhomme']
#Bowlers = ['S Nadeem','TA Boult','Avesh Khan','LE Plunkett','A Mishra','GJ Maxwell']

Overs = ['TG Southee','UT Yadav','TG Southee','UT Yadav','Mohammed Siraj','UT Yadav','YS Chahal','Mohammed Siraj','YS Chahal','Washington Sundar','YS Chahal','C de Grandhomme','YS Chahal','C de Grandhomme','UT Yadav','C de Grandhomme','Mohammed Siraj','TG Southee','Mohammed Siraj','TG Southee']
#Overs = ['S Nadeem','TA Boult','Avesh Khan','LE Plunkett','A Mishra','Avesh Khan','A Mishra','LE Plunkett','TA Boult','GJ Maxwell','LE Plunkett','TA Boult']
Health = {name:1 for name in Batsmen}
batcnt = 0
Runs = 0
Wkt = 0
Strike_bat = Batsmen[batcnt]
batcnt+=1
Off_Striker = Batsmen[batcnt]
Batsmen_scores = {name:[0,0] for name in Batsmen}  #Runs, balls faced
Bowler_wickets = {name:[0,0,0] for name in Bowlers}  #Wickets, Runs given
for X in range(len(Overs)):
    print ("Over:" + str(X+1) + '\n')
    Cur_Bowler = Overs[X]
    Prob = [-1,-1]
    if Strike_bat in Batsman_vul_dict:
        if Cur_Bowler in Batsman_vul_dict[Strike_bat]:
            Prob = (Batsman_vul_dict[Strike_bat][Cur_Bowler])
        else:
            Prob = [-1,-1]
    else:
        Batsman_vul_dict[Strike_bat] = {}
        Prob = [-1,-1]
    if Prob[0]==-1:
        Prob = getProb(Strike_bat,Cur_Bowler)
        Batsman_vul_dict[Strike_bat][Cur_Bowler] = Prob
    for ball in range(6):    #6 balls in one over
        R_num = random()
        init = 0
        Health[Strike_bat] *= Prob[7]
        if (Health[Strike_bat] >=0.5):
            for it in range(7):
                if (init <= R_num < Prob[it]):
                    Runs += it
                    Batsmen_scores[Strike_bat][0]+=it
                    Batsmen_scores[Strike_bat][1]+=1
                    Bowler_wickets[Cur_Bowler][1]+=it  #Runs added to bowler
                    Bowler_wickets[Cur_Bowler][2]+=1   #Add a ball bowled by bowler
                    print (str(X) + '.' + str(ball+1) + " : " + Cur_Bowler + " to " + Strike_bat + ", " + str(it) + " runs.")
                    if(it==1 or it==3 or it==5):
                        Strike_bat,Off_Striker=Off_Striker,Strike_bat
                        #if Strike_bat in Batsmen_vul_dict:
                        if (Strike_bat in Batsman_vul_dict and Cur_Bowler in Batsman_vul_dict[Strike_bat]):
                            Prob = (Batsman_vul_dict[Strike_bat][Cur_Bowler])
                        else:
                            Prob = [-1,-1]
                        if Prob[0]==-1:
                            Prob = getProb(Strike_bat,Cur_Bowler)
                            Batsman_vul_dict[Strike_bat] = {}
                            Batsman_vul_dict[Strike_bat][Cur_Bowler] = Prob
                    break
                else:
                    init = Prob[it]
        else:
            Batsmen_scores[Strike_bat][1]+=1  #Incrementing ball faced by batsman
            Bowler_wickets[Cur_Bowler][2]+=1  #Incrementing ball of bowler
            print (str(X) + '.' + str(ball+1) + " : " + Cur_Bowler+" to "+Strike_bat+", OUT....."+Strike_bat+" made "+str(Batsmen_scores[Strike_bat][0])+" runs off " + str(Batsmen_scores[Strike_bat][1]) + " balls.")
            batcnt += 1
            Strike_bat = Batsmen[batcnt]
            if Strike_bat in Batsman_vul_dict:
                if Cur_Bowler in Batsman_vul_dict[Strike_bat]:
                    Prob = (Batsman_vul_dict[Strike_bat][Cur_Bowler])
                else:
                    Prob = [-1,-1]
            else:
                Prob = getProb(Strike_bat, Cur_Bowler)
            if Prob[0]==-1:
                Prob = getProb(Strike_bat,Cur_Bowler)
                Batsman_vul_dict[Strike_bat][Cur_Bowler] = Prob
            Wkt+=1
            Bowler_wickets[Cur_Bowler][0]+=1
        if (Wkt==10):
            break
    print ("Score at end of over " + str(X+1) + " is: " + str(Runs) + "/" + str(Wkt) + '\n')
    if (Wkt==10):
        break
    Strike_bat,Off_Striker=Off_Striker,Strike_bat
print (" Batting card")
print (Batsmen_scores)
print (" Bowling card")
for B in list(Bowler_wickets.keys()):
    Bowler_wickets[B][0],Bowler_wickets[B][1],Bowler_wickets[B][2] = Bowler_wickets[B][2]//6 + (Bowler_wickets[B][2]%6)/10, Bowler_wickets[B][1],Bowler_wickets[B][0] 
print (Bowler_wickets)

