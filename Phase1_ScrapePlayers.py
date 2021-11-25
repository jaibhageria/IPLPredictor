import bs4
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
Master_list = []
Pnames_short = []

#The below is the URLs of all IPL teams' stat page
my_url=['http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=4343;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=4347;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=4344;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=5845;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=4342;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=4788;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=4341;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=4346;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=4787;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=4345;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=5843;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=4340;type=trophy',
        'http://stats.espncricinfo.com/ci/engine/records/averages/batting.html?id=117;team=5143;type=trophy'
       ]
for i in range(len(my_url)):
    uClient = uReq(my_url[i])
    page_html=uClient.read()
    #page_html
    uClient.close()
    page_soup =soup(page_html,"html.parser")


    containers =page_soup.findAll("table",{"class":"engineTable"})

    #player_links = []
    Container = containers[0].findAll('a',href=True)
    for aa in Container:
        #player_links.append((aa['href']))
        Pnames_short.append (aa.text)
        Master_list.append (aa['href'])
#print (Master_list)
#print(len(Master_list))
FinalList = []
FinalNames = []
#Temp = set(Master_list)
#FinalList = list(Temp)
for x in range(len(Master_list)):
    if Master_list[x] not in FinalList:
        FinalList.append(Master_list[x])
        FinalNames.append(Pnames_short[x])
print((FinalList[13]),FinalNames[13])
print(len(FinalList))
print(len(FinalNames))
#len(FinalList) = 531 => 531 players are there
#print (len(Master_list))
    #print(len(player_links))
#print(len(set(Master_list)))
Indices = []
for Link in FinalList:
    K = Link.split('/')
    Pid = (K[len(K)-1].split('.'))[0]
    Indices.append(Pid)
print(len(Indices))  #Total number of players
print (Indices[13])  #PID of MS Dhoni

Indices = []
for Link in FinalList:
    K = Link.split('/')
    Pid = (K[len(K)-1].split('.'))[0]
    Indices.append(Pid)
print(len(Indices))
print (Indices[13])  #PID of MS Dhoni

Master_list = []
Master_list2 = []
ColHead = ['PID','Name','Matches','Inn','NO','Runs','Best','Avg','BF','SR','100','50','4s','6s']
ColHead2 = ['PID','Name','Matches','Inn','Balls','Runs','Wickets','BBI','BBM','Avg','Econ','SR','4W','5W']
Master_list.append(ColHead)
Master_list2.append(ColHead2)

import bs4
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup

#iteration starts here
for i in range(len(Indices)):
    #if (i%10==0):
        #print(i)
    ind = Indices[i]
    pla = FinalNames[i]
    List_player = []
    List_player2 = []
    my_url = 'http://www.espncricinfo.com/india/content/player/' + str(ind) + '.html'
    #my_url='http://www.espncricinfo.com/india/content/player/28081.html'
    #my_url = 'http://www.espncricinfo.com/india/content/player/' + '28081'+ '.html'
    #my_url
    uClient = uReq(my_url)
    page_html=uClient.read()
    #page_html
    uClient.close()
    page_soup =soup(page_html,"html.parser")
    page_soup.title.text

    #scrap func for name 
    containers =page_soup.findAll("div",{"class":"ciPlayernametxt"})
    PlayerName = containers[0].h1.text
    PlayerName = PlayerName.strip()
    #print(PlayerName)
    #print(containers[0].h1.text)

    containers2 =page_soup.findAll("div",{"class":"pnl490M"})
    containers2[0].findAll("p",{"class":"ciPlayerinformationtxt"})

    #code for player full name 
    containers2[0].p.span.text

    # works 
    all_info = containers2[0].findAll("p",{"class":"ciPlayerinformationtxt"})
    all_info[3]

    #player data final 

    PlayerInfo = []
    all_info = containers2[0].findAll("p",{"class":"ciPlayerinformationtxt"})
    for pp in all_info:
        PlayerInfo.append(pp.span.text)
        #print(pp.span.text)
    for p in range(len(PlayerInfo)):
        PlayerInfo[p] = PlayerInfo[p].strip()
    #print (PlayerInfo)

    table = page_soup.findAll("table",{"class","engineTable"})
    bat_stats = table[0]
    bowl_stats = table[1]

    bat_stats

    bat_header = ['Matches','Inn','NO','Runs','Best','Avg','BF','SR','100','50','4s','6s','Ct','St']
    bat_body = []

    L1 = len(bat_stats.findAll("tr",{"class","data1"}))
    bats_t20_data = bat_stats.findAll("tr",{"class","data1"})[L1-1]    #This is because, T20 stat is always the last
    #print (bats_t20_data.findAll("td")[0]
    #temp = bats_t20_data
    #print (temp)
    
    for i in range(1,15):
        bat_body.append((bats_t20_data.findAll("td")[i].text.strip()))
    for i in range(len(bat_body)):
        if '*' in bat_body[i]:
            x = list(bat_body[i])
            x = x[0:len(x)-1]
            x = ''.join(x)
            bat_body[i] = int(x)
        elif '.' in bat_body[i]:
            bat_body[i] = float(bat_body[i])
        elif '-' in bat_body[i] or bat_body[i]=='':
            bat_body[i] = 'NA'  #Handling cases where batsman has never been dismissed
        else:
            bat_body[i] = int(bat_body[i])
        #print (i)
    #print (bat_header)
    #print (bat_body)

    bowl_header = ['Matches','Inn','Balls','Runs','Wickets','BBI','BBM','Avg','Econ','SR','4W','5W','10W']
    bowl_body = []


    L2 = len(bowl_stats.findAll("tr",{"class","data1"}))
    bowl_t20_data = bowl_stats.findAll("tr",{"class","data1"})[L2-1]   #This is because, T20 stat is always the last
    for i in range(1,14):
        bowl_body.append((bowl_t20_data.findAll("td")[i].text.strip()))
    for i in range(len(bowl_body)):
        if '-' in bowl_body[i]:
            bowl_body[i] = 'NA'  #Handling no bowling figures
        elif '.' in bowl_body[i]:  #Handling averages and strike rate
            bowl_body[i] = float(bowl_body[i])
        elif '/' in bowl_body[i]: #Handling bowling figures
            bowl_body[i] = bowl_body[i]  #no change
        else:
            bowl_body[i] = int(bowl_body[i])
    #print (bowl_header)
    #print (bowl_body)
    #print ("\n")
    
    List_player.extend ([ind,pla])
    List_player.extend (bat_body[0:12])
    Master_list.append (List_player)
    List_player2.extend ([ind,pla])
    List_player2.extend (bowl_body[0:12])
    Master_list2.append (List_player2)
    
print (Master_list[13])
print (Master_list2[13])

import pandas as pd
CSV1 = pd.DataFrame(Master_list2)
#CSV1.to_csv('bowl_profile_ipl.csv')
CSV2 = pd.DataFrame (Master_list)
#CSV2.to_csv('bat_profile_ipl.csv')
#print(CSV)