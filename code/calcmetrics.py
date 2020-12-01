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


