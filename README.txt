About the project:

In this project, we go about trying to predict the scores of both the teams of a given IPL 2018 match using first a cumulative probability approach and then a decision tree approach. 
The data used for the above two is ball by ball data of all IPL matches from 2008 to 2017 and also the T20 batting and bowling statistics of around 530 players which were scraped from espncricinfo.com



Pre-requisites to run the project:

1) Python 3 must be installed. Having Anaconda installed is recommended
2) The requisite Python 3 packages like pyspark, bs4, matplotlib, numpy, os, urllib must be installed.
3) The directory ipl_csv and the included CSV files must be placed in the same directory where the python files are placed.

About the files/folders:

ipl_csv = Directory containing ball by ball details of all IPL matches from 2008 to 2017
Screenshots = Contains a few screenshots of the execution of the python files
2018 IPL Matches = Contains a list of 10 IPL Matches on which our files were tested. The text files included in the directory contains the input data to be fed into the Phase2 and Phase3 programs.

How to run the project?

Execute the files in the below mentioned order:

-> Phase1_ScrapePlayers.py
-> Phase1_BatsmenCluster.py
-> Phase1_BowlersCluster.py
-> Phase2_MatchSimulation.py
-> Phase3_MatchSimulation_DecisionTree.py

Done by:

1) Guruprasad M (01FB16ECS126)
2) Jai Agarwal (01FB16ECS144)
3) Krishna Sidharth S (01FB16ECS169)