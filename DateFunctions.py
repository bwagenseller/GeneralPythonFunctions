import datetime
import time
import string
import pytz

"""
This file is a collection of helpful (and common) date functions.

"""

class TimeIt(object):
	"""
	This is a simple class I wrote to help me wall clock times for segments of code.
	
	This class has two modes: traditional start/stop time captures and a stopwatch (that can capture multiple end times and then average the times). For the traditional, 
		the actions are as follows:
	1. Simply defining a variable as this class will initialize it, starting the timer - OR - you can call the 'Start()' method to (re)start the timer.
	2. Stop the timer (by calling the 'Stop()' method).
	3. Get the results in seconds (by calling the 'GetTimeDeltaInSeconds()' method)
	
	To use the stopwatch mode:
	1. Simply defining a variable as this class will initialize it, starting the timer - OR - you can call the 'Start()' method to (re)start the timer.
	2. To stop the timer, save the results, and then re-start the timer, call 'ClickIt()'
	3. Finding Results
		A. To get the average runtime, call 'AverageRuntime()'
		A. To get all of the individual runtimes, call 'ReturnAllTimes()'
		
	Outside of the Start() method, do _not_ mix and match methods (so only call Stop() / GetTimeDeltaInSeconds() **OR** ClickIt() / AverageRuntime() / ReturnAllTimes(), 
		but **NOT** a combination of both groups)
	"""
	def __init__(self):
		#Initialize the instance of the object
		
		self.startTime = time.time() 
		self.endTime = None
		self.times = []
	
	def Start(self):
		#Start the timer and initialize the 'endTime' and 'times' variables
		
		self.startTime = time.time()
		self.endTime = None
		self.times = []
		
	def Stop(self):
		#Stop the timer by setting the endTime variable
		
		self.endTime = time.time()
		
	def GetTimeDeltaInSeconds(self):
		#If the timer was not stopped, do so - then return the time delta.
		
		if (self.endTime is None): self.endTime = time.time()
		return (self.endTime - self.startTime)
	
	def ClickIt(self):
		#Mimic the clicking of a stopwatch - stop the timer, capture the time, and then re-start the timer.
		
		self.times.append(time.time() - self.startTime)
		self.startTime = time.time() 
		
	def AverageRuntime(self):
		#Return the average runtime for the instance.
		
		if (self.endTime is None): self.endTime = time.time()
		
		if (len(self.times) > 0): return sum(self.times) / len(self.times)
		else: return (self.endTime - self.startTime)
		
	def ReturnAllTimes(self):
		#Returns the list of all elapsed times
		return self.times

class TimeToUpdate(object):
		"""
		Sometimes there is a situation where you want to wait a set amount of time before action is taken; this class manages those instances, so if the time has elapsed
		it will tell the calling entity that the time has elapsed and it should perform whatever function it must (while also resetting its internal clock).
		
		This is a basic attempt at this which does not leverage events - this is simply handled by calls to its methods.
		"""
		
		def __init__(self, statusIntervalInSeconds = 60):
			self.startTime = time.time() 
			
			if (unicode(str(statusIntervalInSeconds)).isnumeric() & isinstance(statusIntervalInSeconds, int)):
				#if statusIntervalInSeconds is a number and an integer, set it
				if (statusIntervalInSeconds >= 0):
					self.statusIntervalInSeconds = statusIntervalInSeconds
				else:
					#'statusIntervalInSeconds' cannot be negative; setting to 60
					self.statusIntervalInSeconds = 60			
			else:
				#'statusIntervalInSeconds' is not a number; setting to 60
				self.statusIntervalInSeconds = 60
				
		def CheckVariables(self, statusIntervalInSeconds = ''):
			if (unicode(str(statusIntervalInSeconds)).isnumeric() & isinstance(statusIntervalInSeconds, int)):
				#if statusIntervalInSeconds is a number and an integer, set it
				if (statusIntervalInSeconds >= 0):
					self.statusIntervalInSeconds = statusIntervalInSeconds
				#If negative, 'statusIntervalInSeconds' is kept to current default

			#There is no 'else' for statusIntervalInSeconds as it defaults to '' here, which is how we know the user did not intend to set this variable
			#	putting an 'else' here would interfere with this as the 'is a number' check would fail above
		
		def SetParameters(self, statusIntervalInSeconds = ''):
			"""
			The parameters that can be set are:
			statusIntervalInSeconds - An integer that determines the number of seconds that must elapse before the object determines an update.
			
			This is the same as 'check variables', but to the user this would visually make more sense
			"""
			#
			self.CheckVariables(statusIntervalInSeconds = statusIntervalInSeconds)
		
		def Start(self):
			"""
			Start the update timer.
			"""
			self.startTime = time.time() 			

	
		def TimeToUpdateStatus(self):
			"""
			This method determines if the time has elapsed - it returns a 'True' if it has (false otherwise) and then re-sets the internal clock.
			
			Note that 'True' is only returned once before the internal clock is re-set. 
			"""
			retVal = False
		
			endTime = time.time()
		
			if((endTime - self.startTime) >= self.statusIntervalInSeconds):
				self.startTime = time.time()
				retVal = True
		
			return retVal

		
def CheckIfDateTimeValid(dateAsStr):
	"""
	takes as input a date/time in the format '%Y-%m-%d %H:%M:%S' OR '%Y-%m-%d'
	returns a True if the input is a valid datetime, False otherwise
	
	How this works is it tries to do a simple change of the datetime and then tries to convert back; if it can successfully, the datetime is valid
	"""

	valid = True

	try:
		#replace all spaces and  colons with a dash ('-') 
		dateTimeList = string.replace(string.replace(dateAsStr, ":", "-"), " ", "-").split('-')
	
		if len(dateTimeList) == 3:
			#simply a date
			myTimestamp = datetime.datetime.strptime(dateAsStr, '%Y-%m-%d')
			addTime = myTimestamp + datetime.timedelta(seconds=(-1))
			dummyTime = addTime.strftime('%Y-%m-%d')
		elif len(dateTimeList) == 6:
			#date and time
			myTimestamp = datetime.datetime.strptime(dateAsStr, '%Y-%m-%d %H:%M:%S')
			addTime = myTimestamp + datetime.timedelta(seconds=(-1))
			dummyTime = addTime.strftime('%Y-%m-%d %H:%M:%S')
		else:
			valid = False
	except:
		valid = False

	return valid

def ConvertDateTimeToTimezone(DateTimeAsSTR, from_tz, to_tz, is_dst=None):
	"""
	This function takes in a date/time (as a string), a 'from' timezone, a 'to' timezone, and a dst qualifier (which I dont actually use).
	This function returns a datetime (in string format) converted to the 'to_tz' timezone.
	from_tz / to_tz are a string timezone; there are many acceptable timezones
		Common ones for the US: 'US/Alaska', 'US/Central', 'US/Eastern', 'US/Hawaii', 'US/Mountain', 'US/Pacific', 'US/Pacific-New', 'US/Samoa', 'UTC'
		you can see them all if you do `print pytz.all_timezones`
	"""
	
	convertTO = pytz.timezone(to_tz)
	convertFROM = pytz.timezone(from_tz)
	
	if (CheckIfDateTimeValid(DateTimeAsSTR)):

		dateTimeList = string.replace(string.replace(DateTimeAsSTR, ":", "-"), " ", "-").split('-')
		if len(dateTimeList) == 3:
			#simply a date
			myTimestamp = datetime.datetime.strptime(DateTimeAsSTR, '%Y-%m-%d')
		elif len(dateTimeList) == 6:
			#date and time
			myTimestamp = datetime.datetime.strptime(DateTimeAsSTR, '%Y-%m-%d %H:%M:%S')
		
		from_dt = convertFROM.localize(myTimestamp, is_dst=is_dst)
		
		convertedTimestamp = convertTO.normalize(from_dt.astimezone(convertTO))
	
		if len(dateTimeList) == 3:
			#simply a date
			return convertedTimestamp.strftime('%Y-%m-%d')
		elif len(dateTimeList) == 6:
			#date and time
			return convertedTimestamp.strftime('%Y-%m-%d %H:%M:%S')
	else:
		return None

def AddSecondsToNow(addSeconds = 0, useUTC = 1, returnJustDate = 0):
	#takes as input seconds; returns a string that represents the current time plus the provided seconds
	if useUTC == 1:
		myTimestamp = datetime.datetime.utcnow()
	else:
		myTimestamp = datetime.datetime.now()
	addTime = myTimestamp + datetime.timedelta(seconds=addSeconds)

	if returnJustDate == 1:
		myStrAddTime = addTime.strftime('%Y-%m-%d')
	else:
		myStrAddTime = addTime.strftime('%Y-%m-%d %H:%M:%S')		

	return myStrAddTime

def SubtractSecondsFromNow(subtractSeconds = 0, useUTC = 1, returnJustDate = 0):
	#takes as input seconds; returns a string that represents the current time minus the provided seconds
	if useUTC == 1:
		myTimestamp = datetime.datetime.utcnow()
	else:
		myTimestamp = datetime.datetime.now()
	addTime = myTimestamp + datetime.timedelta(seconds=-1*subtractSeconds)

	if returnJustDate == 1:
		myStrAddTime = addTime.strftime('%Y-%m-%d')
	else:
		myStrAddTime = addTime.strftime('%Y-%m-%d %H:%M:%S')		

	return myStrAddTime


def AddSecondsToDateTime(dateAsStr, addSeconds = 0):
	#takes as input 
	#	a date/time in the format '%Y-%m-%d %H:%M:%S' OR '%Y-%m-%d'
	#	seconds to add to this time
	#returns a string that represents the given datetime plus the provided seconds

	invalid = 0

	try:
		dateTimeList = string.replace(string.replace(dateAsStr, ":", "-"), " ", "-").split('-')
		
		if len(dateTimeList) == 3:
			#simply a date
			myTimestamp = datetime.datetime.strptime(dateAsStr, '%Y-%m-%d')
			addTime = myTimestamp + datetime.timedelta(seconds=addSeconds)
			myStrAddTime = addTime.strftime('%Y-%m-%d')
		elif len(dateTimeList) == 6:
			#date and time
			myTimestamp = datetime.datetime.strptime(dateAsStr, '%Y-%m-%d %H:%M:%S')
			addTime = myTimestamp + datetime.timedelta(seconds=addSeconds)
			myStrAddTime = addTime.strftime('%Y-%m-%d %H:%M:%S')
		else:
			invalid = 1
	except:
		invalid = 1

	if invalid == 1:
		return None
	else:
		return myStrAddTime

def SubtractSecondsFromDateTime(dateAsStr, subtractSeconds = 0):
	#takes as input 
	#	a date/time in the format '%Y-%m-%d %H:%M:%S' OR '%Y-%m-%d'
	#	seconds to subtract from this time
	#returns a string that represents the given datetime minus the provided seconds

	invalid = 0

	try:
		dateTimeList = string.replace(string.replace(dateAsStr, ":", "-"), " ", "-").split('-')
	
		if len(dateTimeList) == 3:
			#simply a date
			myTimestamp = datetime.datetime.strptime(dateAsStr, '%Y-%m-%d')
			addTime = myTimestamp + datetime.timedelta(seconds=(-1)*subtractSeconds)
			myStrAddTime = addTime.strftime('%Y-%m-%d')
		elif len(dateTimeList) == 6:
			#date and time
			myTimestamp = datetime.datetime.strptime(dateAsStr, '%Y-%m-%d %H:%M:%S')
			addTime = myTimestamp + datetime.timedelta(seconds=(-1)*subtractSeconds)
			myStrAddTime = addTime.strftime('%Y-%m-%d %H:%M:%S')
		else:
			invalid = 1
	except:
		invalid = 1

	if invalid == 1:
		return None
	else:
		return myStrAddTime

def StripMinutesAndSecondsFromDatetime(dateTimeAsStr):
	#This function takes a datetime and strips the minutes and seconds out of the time
	#If successful, the datetime is returned minus the minutes and seconds (in string format); if the datetime is invalid, None is returned 

	invalid = 0

	try:
		dateTimeList = string.replace(string.replace(dateTimeAsStr, ":", "-"), " ", "-").split('-')
	
		if len(dateTimeList) == 3:
			#simply a date
			myTimestamp = datetime.datetime.strptime(dateTimeAsStr, '%Y-%m-%d')
			myTimestamp = myTimestamp.strftime('%Y-%m-%d')
		elif len(dateTimeList) == 6:
			#date and time
			myTimestamp = datetime.datetime.strptime(dateTimeAsStr, '%Y-%m-%d %H:%M:%S')
			myTimestamp = myTimestamp.replace(minute=0, second=0)
			myTimestamp = myTimestamp.strftime('%Y-%m-%d %H:%M:%S')
		else:
			invalid = 1
			
	except:
		invalid = 1

	if invalid == 1:
		return None
	else:
		return myTimestamp
	
def PrintTimestampedMsg(printProgressToScreen = False, myMsg = "NULL", useUTC = True, printToFile = None):
	#if printProgressToScreen, a timestamp and the supplied message is printed to the screen
	#if printToFile is an opened file, this will print the message to that file. This can be used as a log
	if (printProgressToScreen):
		if (useUTC):
			myTimestamp = datetime.datetime.utcnow()
		else:
			myTimestamp = datetime.datetime.now()
		myStrTimestamp = myTimestamp.strftime('%Y-%m-%d %H:%M:%S')
		print "{}: {}".format(myStrTimestamp, myMsg)
	
	if printToFile is not None:
		try:
			if (useUTC):
				myTimestamp = datetime.datetime.utcnow()
			else:
				myTimestamp = datetime.datetime.now()
			myStrTimestamp = myTimestamp.strftime('%Y-%m-%d %H:%M:%S')
			printToFile.write("{}: {}\n".format(myStrTimestamp, myMsg))
		except:
			pass

