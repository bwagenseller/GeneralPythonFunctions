import re
import pandas as pd
import numpy as np
import difflib

import DateFunctions as DateFunc
"""
This file holds functions that help match records in one table/dataframe with records in another table/dataframe across variable columns with variable confidence levels.
"""
def findMatches(dataframeA, dataframeB, matchDictionary, matchConfidenceCol = 'matchConfidence', enforceUniqueMatch = 1, matchChallengerToMultipleMasters = 0, saveUnusedFromDataframeA = 1, saveUnusedFromDataframeB = 1, printProgressToScreen = False):
	"""
	Rules
	1. The column names in each dataframe must be unique
	2. The following column names CANNOT be used: origIndexA, origIndexB, matchConfidence, findMatchString
	3. You can use the fuzzy string pattern matching, but its best to compare strings first without the fuzzy part - this is far faster and will most likely eliminate many matches beforehand
	
	This function uses dataframeA as an anchor and matches to B; its assumed that dataframeA has unique keys, although this is not enforced.
	
	Note that this returns a dataframe containing ALL DATA from dataframes A and B; two new columns are also created, origIndexA and origIndexB, which list the original 
	index mappings.  Use these to map one to the other.  If one is NULL, it means there was not a match found for that row
	
	If no records were matched it simply returns an empty dataframe; to check for an empty dataframe from the calling function:
	if (returnedDataFrame.empty): <whatever>
	
	'matchDictionary' takes an object that is formatted by the 'createMatchDictionary' function below.
	
	'matchConfidenceCol' is the column name where the match confidence is stored.
	
	'enforceUniqueMatch' = 1 forces a 1:1 matchup IF a match exists; in other words, if multiple elements of B match A for any given match confidence, only 
	one match is used. Set this to 0 to ignore this.
	
	'matchChallengerToMultipleMasters' is used where the master (dataframeA) to challenger (dataframeB) list is 1:many (meaning B can be shared to multiple As but a single A cannot be shared 
	by multiple Bs), and in these cases we wish to remove the master column (which was already matched) and NOT the corresponding value of dataframeB (as the same value in B can match 
	multiple values in A, but A must only be matched once).   If matchChallengerToMultipleMasters is set to 1, removal of the challenger identification will NOT happen and thus all values in B
	can be used multiple times; this is defaulted to 0. 
	Note this is different from 'enforceUniqueMatch', as 'enforceUniqueMatch' speaks only to ties for a specific match confidence.
	Also note that currently, many:1 or many:many is not allowed at all.  The master MUST be a 1:? 

	'saveUnusedFromDataframeA'/'saveUnusedFromDataframeB' means if there are any unmatched rows from dataframe A (or respectively B), tack on to the end before returning the matches. Default
	is 1 (meaning tack on matches). 	
	"""
	
	#this is done to make sure the original two dataframes are not modified
	dfA = dataframeA.copy()
	dfB = dataframeB.copy()

	#get the original column names from A and B; this will be useful later	
	originalColumnNames = dfA.columns.values #gets all column names
	originalColumnNames = np.append(originalColumnNames,dfB.columns.values)
	
	#append the added indexes to the front
	originalColumnNames = np.append(['origIndexA', 'origIndexB', matchConfidenceCol],originalColumnNames)
	
	#add the index as a column - use these specific names as the findMatches function requires it
	dfA['origIndexA'] = dfA.index
	dfB['origIndexB'] = dfB.index
	
	#now that the master columns are settled, get the sub-dataframe column names and store them too
	dfA_Columns = dfA.columns.values
	dfB_Columns = dfB.columns.values
	
	AtLeastSomeMergedRecords = 0
	
	#cycle through all possible matches
	for i in range(matchDictionary['NumElements']):
		
		beanCount = 0
		findMatchStringColAdded = 0
		#cycle through every single column match for this match grouping to build the boolean statement 
		for x in range(matchDictionary[i]['numberColumnCompares']):
			"""
			If strLikenessPcnt is set, this means we will be attempting a string comparison using a fuzzy match between 
			colA and colB (fuzzy match means the strings do not have to be exactly alike). The user sets strLikenessPcnt to
			be 0 < strLikenessPcnt <= 1, and then a function determines the closest match (determined by strLikenessPcnt)
			
			We will use dfA as the anchor, making another temp column there that will house the matching (or closest) string in B
			we will then simply join on the two, using the new value for 'column A'
			"""
			if (matchDictionary[i][x]['strLikenessPcnt']!=''):
				#Set a new column in A - we will use this column to match for A, then destroy this column when we are finished with it
				#also do B; the only reason we need to give one to B is to make a lower-case column that we can use to match and then destroy at the end
				myTempColA = 'findMatchString' + str(beanCount) + 'A'
				myTempColB = 'findMatchString' + str(beanCount) + 'B'
				dfA[myTempColA] = dfA[matchDictionary[i][x]['colA']].str.lower()
				dfB[myTempColB] = dfB[matchDictionary[i][x]['colB']].str.lower()

				#get the list of potentials from B.  Make SURE you pull back NO NULLS!
				myPotentialMatchList = dfB.loc[(pd.isnull(dfB[myTempColB]) == False),][myTempColB].values.tolist()
								
				#capitalization DOES matter - so make everything lower case
				beanCount += 1
				dfA[myTempColA] = dfA[myTempColA].apply(lambda y: GetClosestStringMatch(y,myPotentialMatchList, removeMatched = 1, myCutoff = matchDictionary[i][x]['strLikenessPcnt']))
				
				#dfA['findMatchString'] now holds the closest match in B - so simply swap out colA for findMatchString and proceed as normal
				#(initially I did not do B, but I found that case-sensitive matters so I implemented lower() and now manipulate B as well)
				matchDictionary[i][x]['colA'] = myTempColA
				matchDictionary[i][x]['colB'] = myTempColB 
				findMatchStringColAdded = 1
			
				
			if x ==0:
				myLeftOn = [matchDictionary[i][x]['colA']]
				myRightOn = [matchDictionary[i][x]['colB']]
			else:
				myLeftOn.append(matchDictionary[i][x]['colA'])
				myRightOn.append(matchDictionary[i][x]['colB'])
		
		#update the merged dataframe	
		tempMatched = pd.merge(dfA, dfB, how='inner', left_on=myLeftOn, right_on=myRightOn)
		tempMatched[matchConfidenceCol] = matchDictionary[i]['matchConfidence']
		
		#we want to make sure to keep ONLY the non-nulls for this particular match - so eliminate rows where the key fields are NULL
		#we should only have to do this with one side - so we will choose A - since if its a match BOTH will be NULL
		for x in range(matchDictionary[i]['numberColumnCompares']): tempMatched = tempMatched.loc[(pd.notnull(tempMatched[matchDictionary[i][x]['colA']])),:]
			
		#IF we used a fuzzy string match earlier, remove the added column by basically only restoring the original columns (all columns outside of findMatchString)
		if (findMatchStringColAdded == 1):
			findMatchStringColAdded = 0
			dfA = dfA[dfA_Columns]
			dfB = dfB[dfB_Columns]
			tempMatched = tempMatched[originalColumnNames]
		
		#if we wish to enforce a unique match between A and B do so 
		if (enforceUniqueMatch==1):
			enforcedDF = tempMatched.groupby('origIndexA').head(1).reset_index(drop=True)[['origIndexA', 'origIndexB']]
			#save the origIndexB that survived the enforced unique match
			origIndexB = enforcedDF['origIndexB'].values.tolist()
			
			#keep ONLY those B indexes that survived
			tempMatched = tempMatched.loc[(tempMatched['origIndexB'].isin(origIndexB)),:]
		
		#if there are at lease some rows matched
		if(tempMatched.shape[0] > 0):
			
			#if nothing has been added to the final merged df
			if AtLeastSomeMergedRecords == 0:
				matched = tempMatched.copy()
			else:
				matched = matched.append(tempMatched,ignore_index = True) #the ignore index is important otherwise it will re-use the index which may not be desirable
				
			AtLeastSomeMergedRecords = 1
			#remove the items just matched from the feeder dataframes; do this by getting a list of FCC IDs for the A and B dataframes, then only keeping the 
			#rows that do NOT exist in the matched data
			#first just get the list of items to be removed
			origIndexA = matched.loc[(matched[matchConfidenceCol] == matchDictionary[i]['matchConfidence']),]['origIndexA'].values.tolist()
			origIndexB = matched.loc[(matched[matchConfidenceCol] == matchDictionary[i]['matchConfidence']),]['origIndexB'].values.tolist()
	
			#if there is anything to remove do so
			if (len(origIndexA) > 0):
				#if the original B index is used, remove	
				dfA = dfA.loc[(dfA['origIndexA'].isin(origIndexA) == False),:]
				"""
				B is handled a bit differently.  In some instances where the master (dfA) to challenger list is 1:many, and in these cases we wish to remove the master column
				(which was already matched) and NOT the corresponding value of dfB (as the same value in B can match multiple values in A, but A must only be matched once).
				if it is indicated that this is the case, do NOT eliminate used B indexes
				so if matchChallengerToMultipleMasters is set to 1, the below will NOT run and thus all values in B can be used multiple times (this is defaulted to 0)				
				"""
				if(matchChallengerToMultipleMasters == 0): dfB = dfB.loc[(dfB['origIndexB'].isin(origIndexB) == False),:]
	
	
	if (AtLeastSomeMergedRecords == 0):
		DateFunc.PrintTimestampedMsg(printProgressToScreen, "Warning: No matches found!")
		
		return pd.DataFrame()
	else:
		#if there are some elements left in either A or B, lump them on the bottom. leave their matchConfidence NULL
		#also check to see if it was indicated that we wish to lump them or not with saveUnusedFromDataframeA/B 
		if((dfA.shape[0] > 0) & (saveUnusedFromDataframeA == 1)): matched = matched.append(dfA,ignore_index = True)
		if((dfB.shape[0] > 0) & (saveUnusedFromDataframeB == 1)): matched = matched.append(dfB,ignore_index = True)
		
		return matched

def createMatchDictionary(CountOfCompairisonsList, confidenceOffset = 0):
	"""
	This function accepts a list of numbers that represent the number of column matches per confidence level and an offset.
	It returns a pre-built dictionary that must be outfitted to perform a match confidence assessment.
	
	"Match Confidence Level" is the level of confidence in the data (as an integer), with the strongest confidence being 1 (a confidence of 0 is undefined).
	There are two numbered groupings here; the first represents a match attempt, the second identifies the pairs of columns in df A and df B that collectively
	identify the match. So in the example below,the first grouping - group 0, which is the highest confidence match - uses 5 different column matches to determine
	if the record in df A and df B is a match.  
	
	CountOfCompairisonsList:	Required. a list of comparison counts per match confidence level; so, for example, if we are comparing 4 columns for confidence 1, and then
								3 columns for confidence 2, this would be a list (4, 3)
	confidenceOffset:			Optional. An integer to offset the first confidence level; default: 0.  For example, if this is not set, the first / most confident 
								level is 1; if this is set to 1, the first / most confident level is 2 (1+1);  if this is set to 5, the first / most confident level 
								is 6 (5+1).
	
	EXAMPLE
	Input: createMatchDictionary([5,3,1])
	
	Output (as a dictionary):
	{
		'NumElements': 3, 
		0: {
			0: {'strLikenessPcnt': '', 'colB': '', 'colA': ''}, 
			1: {'strLikenessPcnt': '', 'colB': '', 'colA': ''}, 
			2: {'strLikenessPcnt': '', 'colB': '', 'colA': ''}, 
			3: {'strLikenessPcnt': '', 'colB': '', 'colA': ''}, 
			4: {'strLikenessPcnt': '', 'colB': '', 'colA': ''}, 
			'numberColumnCompares': 5, 
			'matchConfidence': 1
		}, 
		1: {
	
			0: {'strLikenessPcnt': '', 'colB': '', 'colA': ''}, 
			2: {'strLikenessPcnt': '', 'colB': '', 'colA': ''}, 
			1: {'strLikenessPcnt': '', 'colB': '', 'colA': ''}
			'numberColumnCompares': 3, 
			'matchConfidence': 2
		}, 
		2: {
			0: {'strLikenessPcnt': '', 'colB': '', 'colA': ''}, 
			'numberColumnCompares': 1, 
			'matchConfidence': 3
		}
	}
	
	######Note######
	You do not have to use this method to create the dictionary for the match confidence assessment, but if you do not 
	you must create your own dictionary to pass and the structure MUST be the same, including the three values saved as the empty string! 
	"""
	numOfElements = len(CountOfCompairisonsList)
	myDictionary = {}
	myDictionary['NumElements'] = numOfElements
	for i in range(numOfElements):
		myDictionary[i] = {}
		myDictionary[i]['matchConfidence'] = i + confidenceOffset + 1
		myDictionary[i]['numberColumnCompares'] = CountOfCompairisonsList[i]
		for x in range(CountOfCompairisonsList[i]):
			myDictionary[i][x] = {}
			myDictionary[i][x]['colA'] = ""
			myDictionary[i][x]['colB'] = ""
			myDictionary[i][x]['strLikenessPcnt'] = ""

	return myDictionary

def GetClosestStringMatch(myStr,closestList,removeMatched = 0, myCutoff = .6):
	"""
	This function accepts a string, a list that contains potential closes matches to that string, and a cutoff (the cutoff determines how close the string must be;
	.99 is VERY close, .01 is almost not the same string).
	
	Please note that the get_close_matches function does not handle nulls well - so make SURE closestList has no nulls!
	
	CRITICAL: Do NOT pass through a portion of a dataframe to this function - carve out a list from the dataframe instead! This function removes
	elements that are successfully matched IF removeMatched == 1
	"""
	myReturnedList = difflib.get_close_matches(myStr,closestList,n=1,cutoff=myCutoff)
	if (len(myReturnedList) == 0):
		return np.nan
	else:
		retVal = myReturnedList[0]
		if (removeMatched == 1): closestList.remove(retVal)
		return retVal
	

def RemoveDistractingWords(givenSeries, deletedWords = []):
	"""
	When pattern matching must be done, there may be common words you wish to eliminate to get rid of some noise.
	This function removes the most common phrases (that you specify), as well as stray punctuation and whitespace
	
	It accepts a pd.Series object AND a list 'deletedWords' (which is your listing of deleted words); it returns the data modified.  A .copy() is used so it does not 
	modify the original data
	"""

	internalSeries = givenSeries.copy()

	if internalSeries.shape[0] > 0: 
		
		#Do the oddball characters
		internalSeries = internalSeries.str.replace("[\!\@\#\$\%\^\&\*\(\)\_\-\+\=\[\{\]\}\\\|\;\:\'\"\<\,\>\.\?\/\`\~]+", ' ', case=False)

		#now cycle through the list and get rid of the words to be ignored
		for i in range(len(deletedWords)):
			myRegex = r"(^|\s)" + re.escape(deletedWords[i]) + r"(\s|$)"
			internalSeries = internalSeries.str.replace(myRegex, ' ', case=False)
		
		#clean up all whitespaces that have two+ consecutive characters (make it a single space)
		internalSeries = internalSeries.str.replace("\s+", ' ', case=False)
		
		#clean up whitespace at the very beginning and end
		internalSeries = internalSeries.str.replace("^\s+", '', case=False)
		internalSeries = internalSeries.str.replace("\s+$", '', case=False)
		
		#finally set the entire series to lower case
		internalSeries = internalSeries.str.lower()

	return internalSeries
