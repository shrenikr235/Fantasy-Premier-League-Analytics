#!usr/bin/python3

import json
import os
import socket
import sys
import threading
from threading import Thread
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
import pyspark.sql.types as tp
from pyspark.sql.functions import lit # for accessing pyspark,sql.Column methods



# We take in the dataset paths for players and teams
playercsv = os.path.join("..", "data", "players.csv")
teamscsv = os.path.join("..", "data", "teams.csv")



# get or instantiate SparkContext
sc = SparkContext(master="local[2]",appName="FantasyPremierLeague").getOrCreate()

# entry point to spark sql
ssc = SparkSession(sc)

# applications can create df's from existing rdd
sql = SQLContext(sc)


# Col order != File Column order => erratic addition of data

PSch = tp.StructType(
	[
		tp.StructField(name= 'name',   			dataType= tp.StringType(),   nullable= False),
		tp.StructField(name= 'birthArea',   	dataType= tp.StringType(),   nullable= False),
		tp.StructField(name= 'birthDate',   	dataType= tp.TimestampType(),nullable= False),
		tp.StructField(name= 'foot',   			dataType= tp.StringType(),   nullable= False),
		tp.StructField(name= 'role',   			dataType= tp.StringType(),   nullable= False),
		tp.StructField(name= 'height',   		dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'passportArea',   	dataType= tp.StringType(),   nullable= False),
		tp.StructField(name= 'weight',   		dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'Id', 				dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'numFouls', 		dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'numGoals', 		dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'numOwnGoals', 	dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'passAcc', 		dataType= tp.FloatType(),    nullable= False),
		tp.StructField(name= 'shotsOnTarget', 	dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'normalPasses', 	dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'keyPasses', 		dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'accNormalPasses', dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'accKeyPasses', 	dataType= tp.IntegerType(),  nullable= False),
		tp.StructField(name= 'rating', 			dataType= tp.FloatType(),  	 nullable= False)
	]
)


# teams schema
TeamSch = tp.StructType(
	[
		tp.StructField(name = 'name', dataType = tp.StringType(), nullable = False),
		tp.StructField(name = 'Id', dataType = tp.IntegerType(), nullable = False)
	]
)

# load data
playerrdd = ssc.read.csv(playercsv, schema = PSch, header = True)
teamsrdd = ssc.read.csv(teamscsv, schema = TeamSch, header = True)

sql.registerDataFrameAsTable(playerrdd, "Player")
sql.registerDataFrameAsTable(teamsrdd, "Teams")

# set vals as 0
for i in ['numFouls','numGoals','numOwnGoals','passAcc','shotsOnTarget','normalPasses','keyPasses','accNormalPasses','accKeyPasses']:
	playerrdd = playerrdd.withColumn(i, lit(0))

# initial player ratings are 0.5
playerrdd = playerrdd.withColumn("rating", lit(0.5))


# define match metrics
myMetrics=['Id','normalPasses', 'keyPasses','accNormalPasses', 'accKeyPasses','passAccuracy','duelsWon', 'neutralDuels','totalDuels', 'duelEffectiveness', 'effectiveFreeKicks', 'penaltiesScored', 'totalFreeKicks','freeKick', 'targetAndGoal', 'targetNotGoal','totalShots', 'shotsOnTarget', 'shotsEffectiveness', 'foulLoss', 'ownGoals','contribution']
df = sql.sql("select Id from Player").collect()
x1 = []
# set 0s
for i in df:
	x1.append((i[0],0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
metrdd=ssc.createDataFrame(x1, myMetrics)
sql.registerDataFrameAsTable(metrdd, "Metrics")


#Creating matches dataframe

x2=[]
# match data
cols=['date','label','duration','winner','venue','goals','own_goals','yellow_cards','red_cards']
x2.append((0,0,0,0,0,0,0,0,0))
matrdd = ssc.createDataFrame(x2, cols)
sql.registerDataFrameAsTable(matrdd, "Matches")



# =================
# my functions
# =================
# Function to computer the Pass Accuracy
# Pass signified by eventId = 8
# 1801 -> accurate
# 1802 -> inaccurate
# 302 -> key pass
def retrievePassAccuracy(accNormalPassesCnt, AccKeyPassesCnt, normPassesCnt, keyPassesCnt):
	val = (accNormalPassesCnt+(2*AccKeyPassesCnt)) / \
		(normPassesCnt + (2*keyPassesCnt))
	return val

# Function to calculate player contribution
# Quantifies contribution of a player towards their team's
# performance during the match and must be bound between 0 and 1
def retrievePlayerContrib(passAcc, duelEffec, FKEffectiveness, ShotsOnTarget):
	val = (passAcc + duelEffec +
		   FKEffectiveness + ShotsOnTarget) / 4
	return val


# Function to calculate Player Rating
# Initially the rating of every player is 0.5
def retrievePlayerRating(PlayerPerformance, CurrPlayerRating):
	val = (PlayerPerformance + CurrPlayerRating) / 2
	return val


# Function to get the Chances of Winning for A and B
def winningChance(AStrength, BStrength):
	chance_of_A_winning = ((0.5 + AStrength) -
						   ((AStrength + BStrength)/2))*100
	chance_of_B_winning = 100 - chance_of_A_winning
	return chance_of_A_winning, chance_of_B_winning

# Function to calculate Duel effectiveness
# EID -> 1
# Bound between 0 and 1
# Tag field in event json has
# id = 701 -> duel lost
# id = 702 -> duel neutral
# id = 703 -> duel won
def retrieveDuelEffectiveness(duelWinCnt, NoResCnt, TotalDuelCnt):
	val = (duelWinCnt + (0.5*NoResCnt)) / TotalDuelCnt
	return val


# Function to calculate Free Kick effectiveness
# eventId = 3
# Bound between 0 and 1
# 1801 -> accurate pass
# 1802 -> inaccurate
def retrieveFreeKickEffectiveness(effecFreeKickCnt, penaltyScoreCnt, freeKickTotalCnt):
	val = (effecFreeKickCnt + penaltyScoreCnt) / freeKickTotalCnt
	return val


# Function to calculate Shots effectiveness
def retrieveShotsEffectiveness(targetPlusGoalShotCnt, targetNotGoalsShotCnt, totalShotCnt):
	val = (targetPlusGoalShotCnt +
		   (0.5*targetNotGoalsShotCnt)) / totalShotCnt
	return val



# Function to computer the Pass Accuracy
def retrievePassAccuracy(accNormalPassesCnt, AccKeyPassesCnt, normPassesCnt, keyPassesCnt):
	val = (accNormalPassesCnt+(2*AccKeyPassesCnt)) / (normPassesCnt + (2*keyPassesCnt))
	return val



def metCalc(rdd):
	global metrdd
	global matrdd
	global playerrdd
	global sql

	# list of dictionaries
	RDDs=[json.loads(i) for i in rdd.collect()]
	stored=[]
	for data in RDDs:
		# print(data)
		# eve.txt
		if 'eventId' in data:
			theplayer=data['playerId']
			# all data 
			myDF2=playerrdd.filter(playerrdd.Id == theplayer)
			# 
			if myDF2.collect():

				values=myDF2.collect()[0]
				myDF3=metrdd.filter(metrdd.Id == theplayer)
				metrVals=myDF3.collect()[0]
				EID=data['eventId']
				
				# tag field

				tf=[tfid['id'] for tfid in data['tags']]
				# event id -> pass
				if EID == 8:
					
					# get the below values from the metrdd df
					# if None is the value start at 0
					accNormalPassesCnt=values[16]
					AccKeyPassesCnt=values[17]
					normPassesCnt=values[14]
					keyPassesCnt=values[15]
					MNormalPassCnt=metrVals[3]
					NumAccKeyPassCnt=metrVals[4]
					MNormPassCnt=metrVals[1]
					MKeyPassesCnt=metrVals[2]
					
					if 1801 in tf:
						# accurate pass
						if 302 in tf:
							NumAccKeyPassCnt+=1
							MKeyPassesCnt+=1
						else:
							MNormalPassCnt+=1
							MNormPassCnt+=1
					elif 1802 in tf:
						# inaccurate pass
						MNormPassCnt+=1
					
					elif 302 in tf:
						# key pass
						MKeyPassesCnt+=1
					
					# for ongoing match
					passAccPM = retrievePassAccuracy(MNormalPassCnt, NumAccKeyPassCnt, MNormPassCnt, MKeyPassesCnt)
					metrdd=metrdd.withColumn("passAccuracy",F.when(F.col("Id") == theplayer,passAccPM).otherwise(F.col("passAccuracy")))
					metrdd=metrdd.withColumn("normalPasses",F.when(F.col("Id") == theplayer,MNormPassCnt).otherwise(F.col("normalPasses")))
					metrdd=metrdd.withColumn("keyPasses",F.when(F.col("Id") == theplayer,MKeyPassesCnt).otherwise(F.col("keyPasses")))
					metrdd=metrdd.withColumn("accNormalPasses",F.when(F.col("Id") == theplayer,MNormalPassCnt).otherwise(F.col("accNormalPasses")))
					metrdd=metrdd.withColumn("accKeyPasses",F.when(F.col("Id") == theplayer,NumAccKeyPassCnt).otherwise(F.col("accKeyPasses")))
				
					# player stats
					accNormalPassesCnt += MNormalPassCnt
					AccKeyPassesCnt += NumAccKeyPassCnt
					normPassesCnt += MNormPassCnt
					keyPassesCnt += MKeyPassesCnt
					to_insert = retrievePassAccuracy(accNormalPassesCnt, AccKeyPassesCnt, normPassesCnt, keyPassesCnt)
					playerrdd=playerrdd.withColumn("passAcc",F.when(F.col("Id")==theplayer,to_insert).otherwise(F.col("passAcc")))
					playerrdd=playerrdd.withColumn("normalPasses",F.when(F.col("Id")==theplayer,normPassesCnt).otherwise(F.col("normalPasses")))
					playerrdd=playerrdd.withColumn("keyPasses",F.when(F.col("Id")==theplayer,keyPassesCnt).otherwise(F.col("keyPasses")))
					playerrdd=playerrdd.withColumn("accNormalPasses",F.when(F.col("Id")==theplayer,accNormalPassesCnt).otherwise(F.col("accNormalPasses")))
					playerrdd=playerrdd.withColumn("accKeyPasses",F.when(F.col("Id")==theplayer,AccKeyPassesCnt).otherwise(F.col("accKeyPasses")))
					print('pa',playerrdd.filter(theplayer==playerrdd.Id).collect()[0][12])



				# TODO!
				# EID == 1 -> duels
				# EID == 2 -> foul
				# EID == 3 -> free kick
				# EID == 10 -> shot effectiveness

				# duels
				if EID == 1:
					duelWinCnt = metrVals[6]
					NoResCnt = metrVals[7]
					totalDuels = metrVals[8]

					if 701 in tf:
						totalDuels += 1

					elif 702 in tf:
						# no real result
						# neutral
						NoResCnt += 1
						totalDuels += 1
					elif 703 in tf:
						duelWinCnt += 1
						totalDuels += 1

					duelEffectivenessPM = retrieveDuelEffectiveness(duelWinCnt,NoResCnt,totalDuels)
					
					# for each match
					metrdd=metrdd.withColumn("duelEffectiveness",F.when(F.col("Id")==theplayer,to_insert).otherwise(F.col("duelEffectiveness")))
					metrdd=metrdd.withColumn("totalDuels",F.when(F.col("Id")==theplayer,totalDuels).otherwise(F.col("totalDuels")))
					metrdd=metrdd.withColumn("neutralDuels",F.when(F.col("Id")==theplayer,NoResCnt).otherwise(F.col("neutralDuels")))
					metrdd=metrdd.withColumn("duelsWon",F.when(F.col("Id")==theplayer,duelWinCnt).otherwise(F.col("duelsWon")))
				







				# checking metrics per match
				# player profiles updated.

				myDF2=metrdd.filter(metrdd.Id == theplayer)
				myDF2=playerrdd.filter(playerrdd.Id == theplayer)				
		else:
			print("match")

# Read the streamed data
strc = StreamingContext(sc, 5)

# i/p dstream connected
lines = strc.socketTextStream("localhost", 6100)

# Print streaming data
# lines.pprint()

# metriccalc for every i/p stream
lines.foreachRDD(metCalc)

# start processing.
strc.start()
strc.awaitTermination()  
strc.stop(stopSparkContext=False, stopGraceFully=True)

