import mysql.connector
#import cx_Oracle
import os
import sys
import socket
import datetime
import time
import random
import string
from mysql.connector import pooling
import sqlalchemy

import DateFunctions as DateFunc

import pandas as pd
import numpy as np

import smtplib #Import smtplib for the email sending function
# Here are the email package modules we'll need
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

"""
While its possible to use the base database libraries, I found there are many instances where its beneficial to utilize pre-packaged code as I found myself 
repeating it multiple times; I wrote this class to mitigate that. 

This class uses several variables:
printProgressToScreen - Can be 1 or 0; if this is 1, messages will be printed (to the screen or a file (if specified)); if 0, nothing will be printed to the screen or file.
hostName - The hostname of the database server you are connecting to. Its also possible to put the IP of the server here instead, if you wish.
userName - The login to the database.
userPass - The password for the login to the database.
userDatabase  - The default schema / database that will be used if not specified.
dsn - The connection DSN (for Oracle only)
port - The database port.
vendor - The vendor of the database (MySQL or Oracle)
oracleServiceName - The Oracle service name.
poolName - The pool name used in MySQL connection pools.
poolSize - The pool size used in MySQL connection pools.
useUTC - If we wish to use UTC time in printing message to the screen.
self.cnx_ - The connection object.
self.cnx_engine_ - The connection object as a connection engine.
self.failureDictionary - A dictionary that holds the items to be inserted into the script failure table / email if errors arise. The elements in this dictionary are:
	scriptID - An integer that identifies the running script calling this method. Initializes to 9999 if left blank.
	hostName - The hostname of the server calling this method. If this is not set, the current hostname is used.
	pid - The pid of the running process.  If this is not set, the current pid is used.
	foreignHostType - The type (in words) of the foreign host the script is interacting with (is the script interacting with a foreign server? If so what is the type of server?)
	foreignHostName - The name (in words) of the foreign host the script is interacting with (is the script interacting with a foreign server? If so what is the servers name?)
	foreignHostID - The integer ID of the foreign host the script is interacting with (is the script interacting with a foreign server? If so what is the servers ID, if it has one?)
	dataDateTime - Does the data you are interacting with have a date/time?  If so what is it?
	scriptRunTime - What time did the calling script start running?  This can help identify the script.
	errorString - If there is an error string, save it here.
	errorNum - If there is an error number, save it here.
	additionalNotes - If there are additional notes on the failure, put them here.
	timeoutHit - This is used to flag times when a query was attempted multiple times and finally times out; this is usually only set in the method 'AttemptChangeQuery'.

			
This class can utilize a few environment files (if they exist):
ORACLE_GENERAL_SERVER - A general Oracle database server that you are using.
ORACLE_GENERAL_USER - A login for the general Oracle database.
ORACLE_GENERAL_PW - A password for the login for the general Oracle database.
MYSQL_GENERAL_SERVER - A general MySQL server that you are using.
MYSQL_GENERAL_USER - A login for the general MySQL database.
MYSQL_GENERAL_PW - A password for the login for the general MySQL database.
SENDING_EMAIL_ADDR- The email address used to send mail.
ADMIN_EMAIL_ADDRS - a comma separated list of admin email addresses; if there is a problem these are email addresses that will be contacted.
ADMIN_TEXT_ADDRS - a comma separated list of phone text addresses; if there is a problem these are text messages that will be sent out.
SendEmail_ERROR_NOTIFICATION - If this is (1) email error notifications will be sent.
SMTP_SERVER - The SMTP server address you use for mail.
"""

class DatabaseConnection(object):
	
	def __init__(self, hostName = None, userName = None, userPass = None, userDatabase = None, dsn = None, port = None, vendor = None, oracleServiceName = None, poolName = "mypool", poolSize = 0, printProgressToScreen = False, useUTC = True):
		"""
		Initialize the variables; the default for most are None (with few exceptions, see above)
		"""

		if (type(printProgressToScreen).__name__ == 'bool'):
			#if printProgressToScreen is a boolean
			self.printProgressToScreen = printProgressToScreen
		else:
			self.PrintTimestampedMsg("Warning - 'printProgressToScreen' is not a boolean; setting to False")
			self.printProgressToScreen = False
									
		#check to see if anything is set to '', which is not allowed; if so set to the default
		if (hostName != ''): 
			self.hostName = hostName
		else:
			self.hostName = None

		if (userName != ''): 
			self.userName = userName
		else:
			self.userName = None

		if (userPass != ''): 
			self.userPass = userPass
		else:
			self.userPass = None

		if (userDatabase != ''): 
			self.userDatabase = userDatabase
		else:
			self.userDatabase = None

		if (dsn != ''): 
			self.dsn = dsn
		else:
			self.dsn = None												

		if (vendor != ''): 
			self.vendor = vendor
		else:
			self.vendor = None		

		if (oracleServiceName != ''): 
			self.oracleServiceName = oracleServiceName
		else:
			self.oracleServiceName = None			
			
			
		if ((poolName != '') & (poolName is not None)): 
			self.poolName = poolName
		else:
			self.poolName = "mypool"

		if (unicode(str(port)).isnumeric() & isinstance(port, int)):
			#if port is a number and an integer, set it
			if ((port >= 0) & (port <= 65535)):
				self.port = port
			else:
				self.PrintTimestampedMsg("Warning - 'port' is out of range; setting to 'NULL'")
				
				self.port = None	

		else:
			self.PrintTimestampedMsg("Warning - 'port' is not a number; setting to 'NULL'")
			
			self.port = None
			

		if (type(useUTC).__name__ == 'bool'):
			#if useUTC is a boolean
			self.useUTC = useUTC
		else:
			self.PrintTimestampedMsg("Warning - 'useUTC' is not a boolean; setting to False")
			self.useUTC = False			
			
		if (unicode(str(poolSize)).isnumeric() & isinstance(poolSize, int)):
			#if poolSize is a number and an integer, set it
			if (poolSize > 0):
				self.poolSize = poolSize
			else:
				self.PrintTimestampedMsg("Warning - 'poolSize' is negative; setting to 0")
				
				self.poolSize = 0
		else:
			#if, for whatever reason, poolSize is not a number, just set to 0
			self.PrintTimestampedMsg("Warning - 'poolSize' is not a number; setting to 0")
			self.poolSize = 0
			
		#The failureDictionary must be initialized		
		self.InitializeFailureDictionary()
		
		self.cnx_ = None
		self.cnx_engine_ = None		
			
	def TieredParameterSet(self, hostName = '', userName = '', userPass = '', userDatabase = '', dsn = '', port = '', vendor = '', oracleServiceName = '', poolName = '', poolSize = '', printProgressToScreen = '', useUTC = ''):
		"""
		This function simply sets the object's parameters in a tiered way - it has the ability to alter parameter settings later, after they are first set.
		For the database connection variables, the check looks at the given parameter first; if its not '', it sets this object's value to that. Otherwise, 
			the value will remain unchanged.
		The non-database values are a bit different 
		(As a consequence, this method will not allow a parameter to be set to '')
		
		Note that we don't initialize here - that is done in the init. We assume the init has already been completed.
		"""
		
		if (hostName != ''): self.hostName = hostName
		if (userName != ''): self.userName = userName
		if (userPass != ''): self.userPass = userPass
		if (userDatabase != ''): self.userDatabase = userDatabase
		if (dsn != ''): self.dsn = dsn
		if (vendor != ''): self.vendor = vendor
		if (oracleServiceName != ''): self.oracleServiceName = oracleServiceName
		if (type(printProgressToScreen).__name__ == 'bool'): self.printProgressToScreen = printProgressToScreen
		if (type(useUTC).__name__ == 'bool'): self.useUTC = useUTC
	
		if ((poolName != '') & (poolName is not None)): self.poolName = poolName
					
		#check the numeric items
		self.CheckAndSaveNumericParameters(port = port, poolSize = poolSize)
						
	def CheckAndSaveNumericParameters(self, port = '', poolSize = ''):
		"""
		This function checks the numeric parameters to see if they are acceptable; if not, they are set to the default  
		"""
				
		if ((port != '') & unicode(str(port)).isnumeric() & isinstance(port, int)):
			#if port is a number and an integer, set it
			if ((port >= 0) & (port <= 65535)):
				self.port = port
			else:
				self.PrintTimestampedMsg("Warning - 'Port' out of range; setting to 'NULL'")
				self.port = None			
			
		elif ((port != '') & unicode(str(self.port)).isnumeric() == False):
			#if, for whatever reason, port is not a number, just set to None
			self.PrintTimestampedMsg("Warning - 'Port' is not a number; setting to 'NULL'")
			self.port = None

		if ((poolSize != '') & unicode(str(poolSize)).isnumeric() & isinstance(poolSize, int)):
			#if poolSize is a number and an integer, set it
			if (poolSize > 0):
				self.poolSize = poolSize
			else:
				self.poolSize = 0
				
		elif ((poolSize != '') & unicode(str(self.poolSize)).isnumeric() == False):
			#if, for whatever reason, poolSize is not a number, just set to 0
			self.PrintTimestampedMsg("Warning - 'poolSize' is not a number; setting to 0")
			
			self.poolSize = 0				

	def InitializeFailureDictionary(self, scriptID = None, hostName = None, pid = None, foreignHostType = None, foreignHostName = None, foreignHostID = None, dataDateTime = None, scriptRunTime = None, errorString = None, errorNum = None, additionalNotes = None, timeoutHit = None):

		"""
		The failureDictionary is a dictionary that holds the items to be inserted into the script failure table / email if errors arise.
		"""
		
		self.failureDictionary = {}
		#check to see if anything is set to '', which is not allowed; if so set to the default
		if (scriptID != ''): 
			if(unicode(str(scriptID)).isnumeric() & isinstance(scriptID, int)):
				self.failureDictionary['scriptID'] = scriptID
			else:
				self.failureDictionary['scriptID'] = 9999
		else:
			self.failureDictionary['scriptID'] = 9999
			
		if (pid != ''): 
			if(unicode(str(pid)).isnumeric() & isinstance(pid, int)):
				self.failureDictionary['pid'] = pid
			else:
				self.failureDictionary['pid'] = os.getpid()
		else:
			self.failureDictionary['pid'] = os.getpid()		
			
		if (hostName != ''): 
			self.failureDictionary['hostName'] = hostName
		else:
			self.failureDictionary['hostName'] = socket.gethostname()

		if (foreignHostType != ''): 
			self.failureDictionary['foreignHostType'] = foreignHostType
		else:
			self.failureDictionary['foreignHostType'] = None
			
		if (foreignHostName != ''): 
			self.failureDictionary['foreignHostName'] = foreignHostName
		else:
			self.failureDictionary['foreignHostName'] = None
			
		if (foreignHostID != ''): 
			if(unicode(str(foreignHostID)).isnumeric() & isinstance(foreignHostID, int)):
				self.failureDictionary['foreignHostID'] = foreignHostID
			else:
				self.failureDictionary['foreignHostID'] = None
		else:
			self.failureDictionary['foreignHostID'] = None	
			
		if (DateFunc.CheckIfDateTimeValid(dataDateTime)): 
			self.failureDictionary['dataDateTime'] = dataDateTime
		else:
			self.failureDictionary['dataDateTime'] = None			

		if (DateFunc.CheckIfDateTimeValid(scriptRunTime)): 
			self.failureDictionary['scriptRunTime'] = scriptRunTime
		else:
			self.failureDictionary['scriptRunTime'] = None				
				
		if (errorNum != ''): 
			if(unicode(str(errorNum)).isnumeric() & isinstance(errorNum, int)):
				self.failureDictionary['errorNum'] = errorNum
			else:
				self.failureDictionary['errorNum'] = None
		else:
			self.failureDictionary['errorNum'] = None
			
		if (errorString != ''): 
			self.failureDictionary['errorString'] = errorString
		else:
			self.failureDictionary['errorString'] = None				
			
		if (additionalNotes != ''): 
			self.failureDictionary['additionalNotes'] = additionalNotes
		else:
			self.failureDictionary['additionalNotes'] = None			

		if (timeoutHit != ''): 
			if(unicode(str(timeoutHit)).isnumeric() & isinstance(timeoutHit, int)):
				self.failureDictionary['timeoutHit'] = timeoutHit
			else:
				self.failureDictionary['timeoutHit'] = 0
		else:
			self.failureDictionary['timeoutHit'] = 0
		
	def SetFailureDictionary(self, scriptID = '', hostName = '', pid = '', foreignHostType = '', foreignHostName = '', foreignHostID = '', dataDateTime = '', scriptRunTime = '', errorString = '', errorNum = '', additionalNotes = '', timeoutHit = ''):
		"""
		This can set the failure dictionary variables at any time
		"""
		
		if(unicode(str(scriptID)).isnumeric() & isinstance(scriptID, int)): self.failureDictionary['scriptID'] = scriptID	
		if(unicode(str(pid)).isnumeric() & isinstance(pid, int)): self.failureDictionary['pid'] = pid
		if (hostName != ''): self.failureDictionary['hostName'] = hostName
		if (foreignHostType != ''): self.failureDictionary['foreignHostType'] = foreignHostType			
		if (foreignHostName != ''): self.failureDictionary['foreignHostName'] = foreignHostName
		if(unicode(str(foreignHostID)).isnumeric() & isinstance(foreignHostID, int)): self.failureDictionary['foreignHostID'] = foreignHostID			
		if (DateFunc.CheckIfDateTimeValid(dataDateTime)): self.failureDictionary['dataDateTime'] = dataDateTime
		if (DateFunc.CheckIfDateTimeValid(scriptRunTime)): self.failureDictionary['scriptRunTime'] = scriptRunTime
		if(unicode(str(errorNum)).isnumeric() & isinstance(errorNum, int)): self.failureDictionary['errorNum'] = errorNum			
		if (errorString != ''): self.failureDictionary['errorString'] = errorString			
		if (additionalNotes != ''): self.failureDictionary['additionalNotes'] = additionalNotes
		if(unicode(str(timeoutHit)).isnumeric() & isinstance(timeoutHit, int)):
			if(timeoutHit == 1): self.failureDictionary['timeoutHit'] = timeoutHit
			else: self.failureDictionary['timeoutHit'] = 0
			
			
	def GetConnection(self, vendor, hostName = '', userName = '', userPass = '', userDatabase = '', dsn = '', port = '', oracleServiceName = '', poolName = '', poolSize = ''):	
		"""
		An agnostic GetConnection; only required variable is 'vendor', which can be one of the following:
			MySQL
			Oracle
		"""
		connSuccessful = False
		
		if (vendor == 'MySQL'): connSuccessful = self.GetMySQLConnection(self, hostName = hostName, userName = userName, userPass = userPass, userDatabase = userDatabase, port = port, poolName = poolName, poolSize = poolSize)
		elif (vendor == 'Oracle'): connSuccessful = self.GetOracleConnection(self, hostName = hostName, userName = userName, userPass = userPass, oracleServiceName = oracleServiceName, dsn = dsn, port = port)
		else:
			self.PrintTimestampedMsg("Warning - unknown database vendor '{}'; exiting.".format(vendor))
			
		return connSuccessful
		
	def GetMySQLConnection(self, hostName = '', userName = '', userPass = '', userDatabase = '', port = '', poolName = '', poolSize = ''):
		"""
		This function is built for streamlining a connection to MySQL.
		
		It *can* utilize a pooled connection, which is important for threaded applications which can use MySQL
		setting poolSize to anything greater than 0 will make this connection a pooled connection; I only suggest this for pools that must persist through threads
		
		Parameters:
			Note that even though all parameters default to '', they are run through self.TieredParameterSet(), which will NOT save '' to any parameter; the initializer actually sets to the default if need be, 
				and the only way to update checks to make sure it cannot be set to ''
			
		Sets:
			self.cnx_
		"""
		
		#set the above parameters
		self.TieredParameterSet(hostName = hostName, userName = userName, userPass = userPass, userDatabase = userDatabase, port = port, poolName = poolName, poolSize = poolSize)
				
		connSuccessful = False
		
		if((self.hostName is None) & ('MYSQL_GENERAL_SERVER' in os.environ)): self.hostName = os.environ['MYSQL_GENERAL_SERVER']
	
		if((self.userName is None) & ('MYSQL_GENERAL_USER' in os.environ)): self.userName = os.environ['MYSQL_GENERAL_USER']
		
		if((self.userPass is None) & ('MYSQL_GENERAL_PW' in os.environ)): self.userPass = os.environ['MYSQL_GENERAL_PW']
		
		if(self.port is None):
			self.port = 3306
			self.printProgressToScreen("Warning - no port set; assuming 3306.") 

		#If there is no valid hostname, username, or password, quit
		if ((self.hostName is None) | (self.userName is None) | (self.userPass is None)):
			if (self.hostName is None): self.printProgressToScreen("Warning - no MySQL hostname given; exiting.")
			if (self.userName is None): self.printProgressToScreen("Warning - no MySQL username given; exiting.")
			if (self.userPass is None): self.printProgressToScreen("Warning - no MySQL password given; exiting.")
			
			return False
		
		#build config dictionary
		config = {
			'user': self.userName,
			'password': self.userPass,
			'host': self.hostName,
			'database': self.userDatabase,
			'port': self.port, 
			'raise_on_warnings': False
		}
	
		self.CloseConnection()#make sure all connections are closed
		self.cnx_ = None
	
		try:
			#if pool size > 0, make this pooled; otherwise its a normal connection
			if (self.poolSize == 0):
				self.cnx_ = mysql.connector.connect(**config)
			else:
				self.cnx_ = pooling.MySQLConnectionPool(pool_name = self.poolName, pool_size = self.poolSize, **config)
				
			
		except mysql.connector.Error as err:
			if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
				self.PrintTimestampedMsg("Something is wrong with your user name or password")
			elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
				self.PrintTimestampedMsg("Database does not exist")
			else:
				self.PrintTimestampedMsg(err)
		else:
			connSuccessful = True
	
		if (connSuccessful): self.vendor = "MySQL" 
		else: self.vendor = None
			
		return connSuccessful
	
	def GetOracleConnection(self, hostName = '', userName = '', userPass = '', oracleServiceName = '', dsn = '', port = ''):
		"""
		This function is built for streamlining a connection to an Oracle database.
		
		Parameters:
			Note that even though all parameters default to '', they are run through self.TieredParameterSet(), which will NOT save '' to any parameter; the initializer actually sets to the default if need be, 
				and the only way to update checks to make sure it cannot be set to ''
			
		Sets:
			self.cnx_
		"""		
		
		#set the above parameters
		self.TieredParameterSet(hostName = hostName, userName = userName, userPass = userPass, dsn = dsn, oracleServiceName = oracleServiceName, port = port)		
				
		connSuccessful = False
		if((self.hostName is None) & ('ORACLE_GENERAL_SERVER' in os.environ)): self.hostName = os.environ['ORACLE_GENERAL_SERVER']
	
		if((self.userName is None) & ('ORACLE_GENERAL_USER' in os.environ)): self.userName = os.environ['ORACLE_GENERAL_USER']
		
		if((self.userPass is None) & ('ORACLE_GENERAL_PW' in os.environ)): self.userPass = os.environ['ORACLE_GENERAL_PW']
		
		if(self.port is None):
			self.port = 1521
			self.printProgressToScreen("Warning - no port set; assuming 1521.") 

		#If there is no valid hostname, username, or password, quit
		if (((self.dsn is None) & (self.oracleServiceName is None)) | ((self.dsn is None) & (self.hostName is None)) | (self.userName is None) | (self.userPass is None) | (self.oracleServiceName is None)):
			if (self.userName is None): self.printProgressToScreen("Warning - no Oracle username given; exiting.")
			if (self.userPass is None): self.printProgressToScreen("Warning - no Oracle password given; exiting.")
			if ((self.dsn is None) & (self.oracleServiceName is None)): self.printProgressToScreen("Warning - no Oracle service name given; exiting.")
			if ((self.dsn is None) & (self.hostName is None)): self.printProgressToScreen("Warning - no Oracle hostname nor dsn given; exiting.")			
			
			return False
		
		self.CloseConnection()#make sure all connections are closed
		self.cnx_ = None
			
		try:

			#determine the DSN; use the user provided one first, but if that doesnt exist make one
			if(self.dsn is None): 
				self.PrintTimestampedMsg("Creating DSN to use with the following parameters: host= {}, port = {}, service name = {}".format(self.hostName, self.port, self.oracleServiceName))
				self.dsn = cx_Oracle.makedsn(self.hostName, self.port, self.oracleServiceName)

			self.PrintTimestampedMsg("Connecting to Oracle DB {} with username {}".format(self.hostName, self.userName))							
			self.cnx_ = cx_Oracle.connect(user=self.userName,password=self.userPass, dsn=self.dsn)
				
		except cx_Oracle.DatabaseError as e:
			error, = e.args
			if error.code == 1017:
				self.PrintTimestampedMsg('Oracle connection failed - Please check your credentials.')
			else:
				self.PrintTimestampedMsg('Database connection error: ' + error.message)
		else:
			connSuccessful = True
	
		if (connSuccessful): self.vendor = "Oracle" 
		else: self.vendor = None
			
		return connSuccessful	

	def CloseConnection(self):
		#This function simply closes the connections if they are open
		if (self.cnx_): self.cnx_.close()
		if (self.cnx_engine_): self.self.cnx_engine_.close()
		
	def ChangeOracleDateToYYYYMMDD(self):
		"""
		This function is useful for changing the Oracle datetime to a more useable format ('YYYY-MM-DD HH24:MI:SS')
		"""
		dateFormatSuccess = False
		
		if(self.vendor == "Oracle"):
			try:
				cursor = self.cnx_.cursor()
				cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
				self.cnx_.commit()#this commit is critical!!!
				cursor.close()
			except cx_Oracle.DatabaseError as e:
				error, = e.args
				if error.code == 1017:
					self.PrintTimestampedMsg('Oracle connection failed - Please check your credentials.')
				else:
					self.PrintTimestampedMsg('Database connection error: ' + error.message)
			else:
				dateFormatSuccess = True
		else:
			self.PrintTimestampedMsg('Warning - not an Oracle connection. Cannot set the Oracle date format.')
				
		return dateFormatSuccess				
	
	def TestConnection(self):
		"""
		This function attempts to run a simple check on the connection.  If successful, this returns a True; False otherwise.
		"""
		
		success = False
		
		self.PrintTimestampedMsg("Testing connection...")
		try:
			successPhrase = pd.read_sql(sql="SELECT 'WarNeverChanges' testMsg FROM DUAL", con=self.cnx_).iloc[0,0]
			self.PrintTimestampedMsg("Connection test successful!")
		except:
			self.PrintTimestampedMsg("Connection test failed!")
		else:
			if (successPhrase == 'WarNeverChanges'): success = True
			
		return success			
			
	def PrintTimestampedMsg(self, myMsg = "NULL", printToFile = None):
		"""
		If printProgressToScreen, a timestamp and the supplied message is printed to the screen.
		If printToFile is an opened file, this will print the message to that file. This can be used as a log.
		"""
		if (self.printProgressToScreen):
			if (self.useUTC):
				myTimestamp = datetime.datetime.utcnow()
			else:
				myTimestamp = datetime.datetime.now()
			myStrTimestamp = myTimestamp.strftime('%Y-%m-%d %H:%M:%S')
			print "{}: {}".format(myStrTimestamp, myMsg)
		
		if printToFile is not None:
			try:
				if (self.useUTC):
					myTimestamp = datetime.datetime.utcnow()
				else:
					myTimestamp = datetime.datetime.now()
				myStrTimestamp = myTimestamp.strftime('%Y-%m-%d %H:%M:%S')
				printToFile.write("{}: {}\n".format(myStrTimestamp, myMsg))
			except:
				pass				
			
	def GetConnectionEngine(self, hostName = '', userName = '', userPass = '', vendor = 'MySQL'):
		"""
		This is a bit different than a connection - its used for the pandas to_sql method almost exclusively; otherwise its not needed
		
		As of now only MySQL is supported - I will try to add Oracle support in the future
		
		Sets:
			self.cnx_engine_
		"""
	
		connectionEngineSuccess = False
		
		#set the above parameters
		self.TieredParameterSet(hostName = hostName, userName = userName, userPass = userPass)
	
		if(vendor == "MySQL"):
			try:
				if((self.hostName is None) & ('MYSQL_GENERAL_SERVER' in os.environ)): self.hostName = os.environ['MYSQL_GENERAL_SERVER']
		
				if((self.userName is None) & ('MYSQL_GENERAL_USER' in os.environ)): self.userName = os.environ['MYSQL_GENERAL_USER']
			
				if((self.userPass is None) & ('MYSQL_GENERAL_PW' in os.environ)): self.userPass = os.environ['MYSQL_GENERAL_PW']			
	
				#return sqlalchemy.create_engine('mysql+mysqlconnector://' + userName + ":" + userPass + "@" + hostName)#, pool_recycle=7200)
				self.cnx_engine_ = sqlalchemy.create_engine('mysql+mysqlconnector://' + userName + ":" + userPass + "@" + hostName, pool_size=100, pool_recycle=7200)#pool_recycle=3600			
	
			except mysql.connector.Error as err:
				if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
					self.PrintTimestampedMsg("Something is wrong with your user name or password")
				elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
					self.PrintTimestampedMsg("Database does not exist")
				else:
					self.PrintTimestampedMsg(err)
					
				self.cnx_engine_ = None
			else:
				connectionEngineSuccess = True
		else:
			self.PrintTimestampedMsg('Warning - not a MySQL connection. Cannot create the connection engine.')
			self.cnx_engine_ = None
					
			return connectionEngineSuccess				
		
		
	def AttemptChangeQuery(self, SQL_Statement, SQL_Data=None, seqNum = None):
		
		"""
		NOTE: This function returns cursor.lastrowid which is the auto_increment generated; that said, THIS DOES NOT WORK WITH INSERTS THAT INSERT MULLTIPLE VALUES AT ONCE - ONLY THE FIRST IS RETURNED
		If a -1 is returned, the query failed
	
		The purpose of this function is to re-try UPDATEs / DELETEs that fail due to deadlocks (specifically for InnoDB tables).
		Basically, if two queries are run at the same time and one locks table 'A' and needs 'B' and the other locks 'B' and needs 'A',
		there is a deadlock.  MySQL will drop the query that is updating less rows immediately when this happens.  When it does happen,
		the message 'Deadlock found when trying to get lock; try restarting transaction' is given in the error message; if this happens,
		use recursion to re-run the query (after waiting a few seconds to give the other query time to finish.  This function allows the 
		user to set the number or attempts and the number of seconds between re-attempts.
	
		passed variables:
		SQL_Statement (required) - The SQL statement to be run.  If anything other than an INSERT...VALUES the entire statement will be held here; if an INSERT...VALUES, this is the INSERT 
			portion and must be parameterized using %s in the parameter spots
		SQL_Data (required if an INSERT) - this is a tuple that represents the values to be inserted (note: NOT from another table, ONLY free-floating values). 
		seqNum (optional) - this is the number of times the function has been called recursively.  This usually should not be set by the user as its used by the recursion process to track counts.  
			Unless you know what you are doing, leave this blank.  If you do know what you are doing, you will usually leave this blank.
		"""

	
		if SQL_Data is None: SQL_Data=[]
		if seqNum is None: seqNum=0
	
		arudMAX = 100
		arudSleepTime = 1
	
		#add some randomization so we dont get continuous locks
		randN = random.randint(0,10)
		arudSleepTime = arudSleepTime + randN
	
		arudReturn = 0 
		
	
		#if the count of times this function has been called is under the max number or allowable attempts, continue
		if seqNum < arudMAX:
			try:
				cursor = self.cnx_.cursor()
				if len(SQL_Data) == 0:
					cursor.execute(SQL_Statement)
				else:
					#multiple things to insert
					cursor.executemany(SQL_Statement, SQL_Data)
				self.cnx_.commit()#this commit is critical!!!
			except mysql.connector.Error as err:
				
				if err.errno in (mysql.connector.errorcode.ER_LOCK_WAIT_TIMEOUT, mysql.connector.errorcode.ER_LOCK_DEADLOCK):
					#errors 1205, 1213
					#if the deadlock message was reached, increase the count that tracks how many times it ran, sleep for a bit, and then call this function again via recursion
					seqNum = seqNum + 1
					time.sleep(arudSleepTime)
					arudReturn = self.AttemptChangeQuery(SQL_Statement, SQL_Data, seqNum)#recursion
					
				elif(err.errno == 2055):
					#errors 2055: Lost connection to MySQL server at '', system error: 32 Broken pipe
					self.PrintTimestampedMsg(err.msg)
					self.SetFailureDictionary(errorString = err.msg, errorNum = err.errno)
					self.ScriptErrorNotification()
					arudReturn = -1 					
				else:
					#if there was an error but it was not the deadlock error, record it, email the admins, and return 0
					self.PrintTimestampedMsg(err.msg)
					self.SetFailureDictionary(errorString = err.msg, errorNum = err.errno)
					self.cnx_.rollback()
					self.ScriptErrorNotification()
					arudReturn = -1 
			else:
				#query successful
				arudReturn = cursor.lastrowid
		else:
			#arudMAX was hit - this should not happen
			self.SetFailureDictionary(additionalNotes = "Max number of retries hit {}".format(arudMAX), timeoutHit = 1)
			self.ScriptErrorNotification()
			arudReturn = -1 
	
		cursor.close()
		return arudReturn
	
	def AttemptDataFrameUpload(self, uploadingDataFrame, tableName, schemaName = "TEMP", rowCountChunkSize = 5000, ifTableExists = 'append', connectionEngine = None, seqNum = None):
		"""
		The purpose of this function is to act very similarly to the AttemptChangeQuery function (above, the main focus being avoiding deadlocks in MySQL),
		but instead of running a simple SQL command the goal of this function is to push an entire dataframe to a table at once via the 'sqlalchemy' package.
		As such, there is no query nor data; instead, a dataframe, the schema, and the table (separate from the schema) is needed. Also this does NOT take a 
		connection object, like most SQL in Python; instead, it takes a connection engine object (see function 'GetConnectionEngine' above which makes 
		such a connection). This does not seem to act as a constant connection; rather, it works on an as-needed basis.
		It should be noted that this utilizes 'sqlalchemy' and sqlalchemy has major problems with timing out on longer-running queries (anything above 10 
		seconds, it seems) - there are 'Pipe Broken' error messages abound. To combat this, we can chunk a dataframe into smaller pieces (rowCountChunkSize)  
		and pass it like that, which gets around the limitation while still utilizing the vectorized code of sqlalchemy. 
				
		This used to require the index of the dataframe to be an integer and sequential - so no special dataframe indexes could be used. Since I converted this to use .iloc,
		this may no longer be true. 
	
		passed variables:
		uploadingDataFrame (required) - the dataframe that is to be uploaded. NOTE A TRUNCATION IS DONE BY DEFAULT ON THIS TABLE SO BE CAREFUL. Also please 
			make sure that you have pre-created this table
		tableName (required) - The name of the table you are uploading to. If it does not exist it will be created. 
		schemaName (not required, Default: "TEMP") - this is the schema where the table will be loaded to.
		rowCountChunkSize (not required but it must be a positive number; Default: 5000): This is the number of rows sent to the table at once. Be careful with
			this, as if its too big A) it will time out due to sqlalchemy being not up to snuff or B) the amount of data you are trying to load to MySQL may be
			too much to load at once
		ifTableExists (optional; Default: "append"): This corresponds with the pandas.DataFrame.to_sql 'if_exists' parameter. Values are 'fail' (if table exists,
			dump out), 'replace' (re-create the table), or 'append' (if table exists simply insert the data)
		connectionEngine (optional; Default: self.GetConnectionEngine()): A connection engine for sqlalchemy
		seqNum (optional) - this is the number of times the function has been called recursively.  This usually should not be set by the user as its used by the recursion process to track counts.  
			Unless you know what you are doing, leave this blank.  If you do know what you are doing, you will usually leave this blank.

		"""
	
		if seqNum is None: seqNum=0
		if connectionEngine is None: connectionEngine = self.GetConnectionEngine()
	
		arudMAX = 100
		arudSleepTime = 1
		
		#this creates a listener to detect if the connection engine experiences the database 'went away' due to the pipe breaking, etc; checkout_listener is defined below 
		from sqlalchemy import event
		event.listen(connectionEngine, 'checkout', checkout_listener)
	
		#add some randomization so we dont get continuous locks
		randN = random.randint(0,10)
		arudSleepTime = arudSleepTime + randN
	
		arudReturn = 0 
	
		#if the count of times this function has been called is under the max number or allowable attempts, continue
		if seqNum < arudMAX:
			for onDeckPos in range(0,uploadingDataFrame.shape[0],rowCountChunkSize):
				upperBound = onDeckPos + rowCountChunkSize
				if upperBound > uploadingDataFrame.shape[0]: upperBound = uploadingDataFrame.shape[0]
				self.PrintTimestampedMsg("Importing lines " + str(onDeckPos) + " to " + str(upperBound) + "...")	
				attemptedDataFrame = uploadingDataFrame.iloc[onDeckPos:upperBound,].copy()
				
				try:
					attemptedDataFrame.to_sql(tableName, connectionEngine, schema = schemaName, index=False, if_exists = ifTableExists)
					
				except:
					"""
					Here is where the error handling  - and recursion - could happen.  See AttemptChangeQuery as an example (in the first 'if' statement in particular - here
					#I just use the 'else' which is when it fails)
		
					if there was an error but it was not the deadlock error, record it, email the admins, and return 0
					"""
	
					errMsg = str(sys.exc_info()[0]) + " --- " + sys.exc_info()[1][0]
					self.PrintTimestampedMsg(errMsg)
					self.SetFailureDictionary(errorString = errMsg, errorNum = -1)
					self.ScriptErrorNotification()
					arudReturn = -1 
					break #exit FOR loop
					"""
					######################################################################################################################
					######################################################################################################################
					######################################################################################################################
					######################################################################################################################
					######################################################################################################################
					###########################IF I EVER PUT IN A RECURSIVE FUNCTION, I MUST TAKE THE BREAK OUT###########################
					######################################################################################################################
					######################################################################################################################
					######################################################################################################################
					######################################################################################################################
					######################################################################################################################
					"""
				else:
					#query successful
					arudReturn = 1
	
		else:
			#arudMAX was hit - notify the admins as this should not happen
			self.SetFailureDictionary(additionalNotes = "Max number of retries hit {}".format(arudMAX), timeoutHit = 1)
			self.ScriptErrorNotification()
			arudReturn = -1 
			
		return arudReturn
	
	def CreateTempTable(self, gutsOfCREATE, dateTimeOfData = DateFunc.AddSecondsToNow(returnJustDate = 1), tempSchemaName = "TEMP", nodeName = None, description = None, InMemory = 1):
		"""
		This function creates a temp table that can be used for populating a larger table later
		If successful the name of the table created is returned - otherwise, a None is returned
		
		Variables:
		gutsOfCREATE - this is the 'meat' of the create statement that MUST be supplied; for example, this is 'XXX' of 'CREATE TABLE ABC (XXX) ENGINE=MEMORY
		dateTimeOfData - the datetime of the data; usually the timeframe of the data being pulled
		tempSchemaName - The schema where the table will be created; if left blank, this will be the "TEMP" schema
		nodeName - the nodename of the element that is represented by this table
		description - a description of this temp table
		
		REQUIRES: a Schema named 'TEMP'
		"""
		hostName =  socket.gethostname()
		hostNameMod = hostName.replace("-", "_") #table names CANNOT use a dash - convert to underscore
	
		tempBaseTableName = datetime.datetime.utcnow().strftime('%Y_%m_%d_%H_%M_%S_PID_')  + str(os.getpid()) + "_" + str(self.failureDictionary['scriptID']) + "_" + hostNameMod
		tempTableName = "`" + tempSchemaName + "`.`" + tempBaseTableName + "`" 
	
		#actually create temp table - make it in volatile memory
		if (InMemory == 1):
			"""
			if we want this table to be built in memory, do so - otherwise do not build in memory
			it may be advantageous to avoid the memory engine if the table is huge - it will error out with a '1114: table is filled' error. To avoid this do not use memory
			"""
			SQL = "CREATE TABLE {} ({}) ENGINE=MEMORY".format(tempTableName, gutsOfCREATE)
		else:
			SQL = "CREATE TABLE {} ({})".format(tempTableName, gutsOfCREATE)
		createTableSucc = self.AttemptChangeQuery(SQL)
	
		#track this temp table
		SQL = "INSERT INTO HouseData.TempTableTracker (UTC_DateTime, HostName, pid, scriptID, tempSchemaName, tempTableName, targetNodeName, DateTimeOfData, Description) VALUES (UTC_TIMESTAMP(), %s, %s, %s, %s, %s, %s, %s)"
		SQL_Data = []
		SQL_Data.append((hostName, os.getpid(), self.failureDictionary['scriptID'], tempSchemaName, tempBaseTableName, nodeName, dateTimeOfData, description))
		
		tableTrackerSucc = self.AttemptChangeQuery(SQL, SQL_Data)
		
		if ((createTableSucc != -1) & (tableTrackerSucc != -1)):
			return tempBaseTableName
		else:
			return None
	
	def SleepTestQuery(self, SleepSecondsMin = 0, SleepSecondsMax=15):
		"""
		This simply tests the connection with a sleep() command; its can be useful for partially simulating multiple queries at once.
		
		If there are no problems this returns true, otherwise false
		"""
	
		try:
			SQL = "SELECT SLEEP({}) aa;".format(random.randint(SleepSecondsMin,SleepSecondsMax))
			myCursor = self.cnx_.cursor()
			myCursor.execute(SQL)
			for (aa) in myCursor: trash = aa
			myCursor.close()		
		except:
			return False
		else:
			return True
		
	def GetMaxDateInTable(self, nameOfSchemaDotTableName, nameOfDateColumn, searchUsingPrevDays = 3, minRowCount = 1, truncateToDateOnly = 1, forcedDate = None):
		"""
		The main goal of this function is to find the max date available in a given table.
		
		This function takes the name of a table (nameOfSchemaDotTableName, in 'schema.table' format), and the name of the column that houses
		the date (nameOfDateColumn) to accomplish the above goal. 
		
		IT SHOULD BE NOTED that this function assumes Oracle connections use the date structure YYYY-MM-DD (which is set by the function ChangeOracleDateToYYYYMMDD
		in this python file). 
		
		'truncateToDateOnly' will lop off the time portion, if it exists. Note that if this is set to 0 you CANNOT use the minRowCount or searchUsingPrevDays options, as sometimes data is stored with a time and this must be truncated
		
		'minRowCount' will be used to find the max date with at least minRowCount number of records. ***NOTE THIS IS CURRENTLY NOT IMPLEMENTED***
			
		'searchUsingPrevDays' is the max number of days to look in the past; default 3
		
		'forcedDate' is used (format: YYYY-MM-DD) when you wish to use a SPECIFIC date; if you do not and would instead like to use the range, leave this None (or do not set).   
		
		If a max exists it is returned; otherwise, a np.nan is returned
		
		NOTES:
		Its possible to use this for a general MAX (numbers, floats, etc) but be careful with this; also make SURE truncateToDateOnly = 0 otherwise this will error out
		The record count functionality works best of the date fields are TRULY date fields; sometimes people call a column a date but its actually a datetime, 
			and this could confuse this function when used in the record count
		
		Returns:
		This function returns the date - in string format YYYY-MM-DD - if it exists; 'None' otherwise
		"""
		
		#if minRowCount is less than 1 set it to 1
		if (minRowCount < 1): minRowCount = 1
		
		myCursor = self.cnx_.cursor()
		
		#If we want to truncate to the date only
		if (truncateToDateOnly ==1):
			if (self.vendor == "MySQL"):
				#MySQL
				SQL = "SELECT DATE_FORMAT(" + nameOfDateColumn + ", '%Y-%m-%d') dataDate, count(*) myRecordCount FROM " + nameOfSchemaDotTableName + " WHERE "
				if (forcedDate is None):
					SQL = SQL + nameOfDateColumn + " >= DATE_SUB(CURDATE(), INTERVAL " + str(searchUsingPrevDays) + " DAY) GROUP BY DATE_FORMAT(" + nameOfDateColumn + ", '%Y-%m-%d')"
				else:
					SQL = SQL + "DATE_FORMAT(" + nameOfDateColumn + ", '%Y-%m-%d') = '" + forcedDate + "' GROUP BY DATE_FORMAT(" + nameOfDateColumn + ", '%Y-%m-%d')"
	
			elif (self.vendor == "Oracle"):
				#Oracle
				SQL = "SELECT TO_CHAR(" + nameOfDateColumn + ", 'YYYY-MM-DD') dataDate, count(*) myRecordCount FROM " + nameOfSchemaDotTableName + " WHERE "
				if (forcedDate is None):
					SQL = SQL + nameOfDateColumn + " >= (sysdate - " + str(searchUsingPrevDays) + ") GROUP BY TO_CHAR(" + nameOfDateColumn + ", 'YYYY-MM-DD')"
				else:
					SQL = SQL + "TO_CHAR(" + nameOfDateColumn + ", 'YYYY-MM-DD') = '" + forcedDate + "' GROUP BY TO_CHAR(" + nameOfDateColumn + ", 'YYYY-MM-DD')"

				
			SQL = ("SELECT dataDate, myRecordCount FROM (" + SQL + ") myInnerTable WHERE myRecordCount >= " + str(minRowCount) + " ORDER BY dataDate DESC")
			
		else:
		#if we are keeping the datetime intact
			if (self.vendor == "MySQL"):
				#MySQL
				SQL = "SELECT " + nameOfDateColumn + " dataDate, count(*) myRecordCount FROM " + nameOfSchemaDotTableName + " WHERE "
				if (forcedDate is None):
					SQL = SQL + nameOfDateColumn + " >= DATE_SUB(CURDATE(), INTERVAL " + str(searchUsingPrevDays) + " DAY) GROUP BY " + nameOfDateColumn
				else:
					SQL = SQL + nameOfDateColumn + " = '" + forcedDate + "' GROUP BY " + nameOfDateColumn
	
			elif (self.vendor == "Oracle"):
				#Oracle
				SQL = "SELECT " + nameOfDateColumn + " dataDate, count(*) myRecordCount FROM " + nameOfSchemaDotTableName + " WHERE "
				if (forcedDate is None):
					SQL = SQL + nameOfDateColumn + " >= (sysdate - " + str(searchUsingPrevDays) + ") GROUP BY " + nameOfDateColumn
				else:
					SQL = SQL + nameOfDateColumn + " = '" + forcedDate + "' GROUP BY " + nameOfDateColumn
				
			SQL = ("SELECT dataDate, myRecordCount FROM (" + SQL + ") myInnerTable WHERE myRecordCount >= " + str(minRowCount) + " ORDER BY dataDate DESC")
	
	
		dataDate = None
		myRecordCount = None
		
		#cycle though and get the top data date
		myCursor.execute(SQL)
		for (foundDataDate, foundRecordCount) in myCursor: 
			if (dataDate == None): dataDate = foundDataDate
			if (myRecordCount == None): myRecordCount = foundRecordCount
		
		
		#make sure the dataDate is a str
		if ((truncateToDateOnly ==1) & (dataDate is not None) & isinstance(dataDate, datetime.datetime)):
			dataDate = dataDate.strftime('%Y-%m-%d')
		elif ((truncateToDateOnly ==0) & (dataDate is not None) & isinstance(dataDate, datetime.datetime)):
			#same as above but handles the time portion as well
			dataDate = dataDate.strftime('%Y-%m-%d %H:%M:%S')
		
		myCursor.close()	
		return dataDate, myRecordCount
	
	
	def ScriptErrorNotification(self):
		SendEmail_ERROR_NOTIFICATION = os.environ['SendEmail_ERROR_NOTIFICATION']
		ADMIN_TEXT_ADDRS = os.environ['ADMIN_TEXT_ADDRS']
		ADMIN_EMAIL_ADDRS = os.environ['ADMIN_EMAIL_ADDRS']
	
		INSERT = ''
		VALUES = ''
		Body = ''
		TxtBody = ''
		dbConnectionError = 0
		myUTC_Time = DateFunc.AddSecondsToNow()
	
		if ((self.failureDictionary['errorNum'] in (mysql.connector.errorcode.ER_SYNTAX_ERROR, mysql.connector.errorcode.ER_PARSE_ERROR)) & (self.vendor == "MySQL")):
			#Error: 1149 SQLSTATE: 42000 (ER_SYNTAX_ERROR)
			#You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use 
			#if this is seen we do not want to record it - so just pass
			pass
		else:
	
			#build the variable string
			INSERT = 'insertTimeUTC, hostServer'
			#build the values string
			VALUES = "UTC_TIMESTAMP(), '" + self.failureDictionary['hostName'] + "'";
	
	
			#Build the remainder of the INSERT and VALIES
			for keyName, myValue in self.failureDictionary.iteritems():
				if ((myValue != '') & (myValue is not None)):
					INSERT += ', ' + keyName
					if keyName in {'hostName', 'foreignHostType', 'foreignHostName', 'dataDateTime', 'scriptRunTime', 'errorString', 'additionalNotes'}:
						myValue = string.replace(myValue, "'", "`") #get rid of all single quotes
						myValue = string.replace(myValue, ">", "") #get rid of > as it can mess with tags
						myValue = string.replace(myValue, "<", "") #get rid of < as it can mess with tags
						VALUES += ", '" + myValue + "'"
					else:
						VALUES += ', ' + str(myValue)
	
			SQL_Statement = "INSERT INTO HouseData.ScriptQueryErrors ({}) VALUES ({})".format(INSERT, VALUES)
			try:
				cursor = self.cnx_.cursor()
				cursor.execute(SQL_Statement)
				self.cnx_.commit()#this commit is critical!!!
			except mysql.connector.Error as err:
				self.PrintTimestampedMsg(err)
				dbConnectionError = 1#if nothing could be inserted there is a DB error - may be down
				
			
			if dbConnectionError == 1: Body = "**NOTE: Database Connction Failure**<br>"
			Body += "<br>Host: " + self.failureDictionary['hostName']
			Body += "<br>UTC Time: " + myUTC_Time
			if self.failureDictionary['scriptID'] != '': Body += "<br>Script ID: " + str(self.failureDictionary['scriptID'])
			if self.failureDictionary['pid'] != '': Body += "<br>PID: " + str(self.failureDictionary['pid'])
			if self.failureDictionary['foreignHostType'] != '': Body += "<br>Foreign Host Type: " + self.failureDictionary['foreignHostType']
			if self.failureDictionary['foreignHostID'] != '': Body += "<br>Foreign Host ID: " + str(self.failureDictionary['foreignHostID'])
			if self.failureDictionary['foreignHostName'] != '': Body += "<br>Foreign Host Name: " + self.failureDictionary['foreignHostName']
			if self.failureDictionary['dataDateTime'] != '': Body += "<br>Data Date Time: " + self.failureDictionary['dataDateTime']
			if self.failureDictionary['timeoutHit'] != '': 
				Body += "<br>Timeout Hit: "
				if self.failureDictionary['timeoutHit'] == 1: Body += 'Yes'
				else: Body += 'No'
			if self.failureDictionary['errorString'] != '': Body += "<br><br>Error String: " + self.failureDictionary['errorString']
			if self.failureDictionary['additionalNotes'] != '': Body += "<br><br>Additional Notes: " + self.failureDictionary['additionalNotes']
	
			if dbConnectionError == 1: TxtBody = "**NOTE: Database Connction Failure**<br>"
			TxtBody += "<br>Host: " + self.failureDictionary['hostName']
			TxtBody += "<br>UTC Time: " + myUTC_Time
			if self.failureDictionary['scriptID'] != '': TxtBody += "<br>Script ID: " + str(self.failureDictionary['scriptID'])
			if self.failureDictionary['foreignHostType'] != '': TxtBody += "<br>Foreign Host Type: " + self.failureDictionary['foreignHostType']
			if self.failureDictionary['foreignHostName'] != '': TxtBody += "<br>Foreign Host Name: " + self.failureDictionary['foreignHostName']
			if self.failureDictionary['dataDateTime'] != '': TxtBody += "<br>Data Date Time: " + self.failureDictionary['dataDateTime']
			if self.failureDictionary['timeoutHit'] != '':
				TxtBody += "<br>Timeout Hit: "
				if self.failureDictionary['timeoutHit'] == 1: TxtBody += 'Yes'
				else: TxtBody += 'No'
	
			#if the global SendEmail_ERROR_NOTIFICATION is set to 1
			if SendEmail_ERROR_NOTIFICATION == '1':
				self.SendEmail(ADMIN_EMAIL_ADDRS,"Query Failure", Body, 1)
				self.SendEmail(ADMIN_TEXT_ADDRS,"Query Failure", TxtBody, 1)
				
	def MultishotQuery(self, QueriesToRun, Descriptions = None):
		"""
		This function is built to handle a list of queries to be run in succession. Note it can NOT handle SELECT queries. 
	
		passed variables:
		QueriesToRun (required) - A list containing the queries you wish to run.  They are launched in the order as presented in this list.
		Descriptions (optional, but recommended) - A list containing the descriptions of the queries you wish to run.  They should be in the same order as the queries.
			These are optional, however without them it could get confusing.
		
		what is returned - Two variables are returned
			the first is a boolean which indicates if it was successful or not. Note a 'success' counts as all queries running.
			the second is a Dataframe with the failed queries; If NoneType is returned the function failed; if a dataframe with no rows is returned all queries were successful; 
				A populated dataframe indicates the failed queries and has the following columns: 
					Queries - the actual provided query of just the failed queries,
					Descriptions - the actual provided descriptions of just the failed queries,
		"""
		
		pleaseContinue = 1
		
		#check to make sure the queries and descriptions have the same number of elements
		if (QueriesToRun is None):
			self.PrintTimestampedMsg("ERROR - There are no queries - exiting.")
			pleaseContinue = 0
			QueriesToRun = [''] #just set to something so it does not error out later - note we are still exiting as pleaseContinue = 0 
		elif (len(QueriesToRun) == 0):
			self.PrintTimestampedMsg("ERROR - There are no queries - exiting.")
			pleaseContinue = 0
			QueriesToRun = [''] #just set to something so it does not error out later - note we are still exiting as pleaseContinue = 0		
		
		if (Descriptions is None):
			self.PrintTimestampedMsg("Warning - no descriptions set.  Setting all to the empty string, which may cause confusion.")
			Descriptions = ['']*len(QueriesToRun)
		elif (len(Descriptions) == 0):
			self.PrintTimestampedMsg("Warning - no descriptions set.  Setting all to the empty string, which may cause confusion.")
			Descriptions = ['']*len(QueriesToRun)		
			
		if(len(QueriesToRun) != len(Descriptions)):
			if (len(QueriesToRun) > len(Descriptions)):
				self.PrintTimestampedMsg("WARNING - the number of queries and descriptions do not match; queries outnumber descriptions. Descriptions may not be correct...")
				for i in range(len(Descriptions),len(QueriesToRun)): Descriptions.append('')
			else:
				self.PrintTimestampedMsg("ERROR - more descriptions than queries - exiting.")
				pleaseContinue = 0
		
		if (pleaseContinue == 1):
			results = pd.DataFrame({'Queries' : QueriesToRun, 'Descriptions' : Descriptions, 'Results' : np.zeros(len(Descriptions))})
			for i in range(len(QueriesToRun)):
				self.PrintTimestampedMsg(results.loc[i, ['Descriptions']].values[0])
				results.loc[i, ['Results']] = self.AttemptChangeQuery(results.loc[i, ['Queries']].values[0])
				if (results.loc[i, ['Results']].values[0] == -1): self.PrintTimestampedMsg("WARNING - Previous query failed!")
			results.loc[(results['Results'] >= 0), ['Results']] = 0
			results.loc[(results['Results'] < 0), ['Results']] = -1
			
			results = results.loc[(results['Results'] == -1), ]
			results = results[['Queries', 'Descriptions']]
	
			#if all queries ran successfully
			if (results.shape[0] == 0):
				self.PrintTimestampedMsg("All queries ran successfully...")
				return True, results
			else:
				#if at least one query failed
				self.PrintTimestampedMsg("WARNING - some queries failed...")
				return False, results
		else:
			return False, None
	
	
	def SendEmail(self, ToEmail, Subject, Body, IsHTML=0):
		#http://stackoverflow.com/questions/6270782/how-to-send-an-email-with-python
		#http://stackoverflow.com/questions/882712/sending-html-email-using-python
	
		SENDING_EMAIL_ADDR = os.environ['SENDING_EMAIL_ADDR']
		SMTP_SERVER = os.environ['SMTP_SERVER']
		
		COMMASPACE = ', '
	
		# Create the container (outer) email message.
		msg = MIMEMultipart('alternative')
		msg['Subject'] = Subject
		msg['From'] = SENDING_EMAIL_ADDR
		msg['To'] = ToEmail #COMMASPACE.join(family)
		
		if IsHTML==1:
			SentBody = MIMEText(Body, 'html')
		else:
			SentBody = MIMEText(Body, 'plain')
	
		msg.attach(SentBody)
	
		"""
		#if we wanted to send an image
		# Assume we know that the image files are all in PNG format
		for file in pngfiles:
			# Open the files in binary mode.  Let the MIMEImage class automatically
			# guess the specific image type.
			fp = open(file, 'rb')
			img = MIMEImage(fp.read())
			fp.close()
			msg.attach(img)
		"""
	
		# Send the email via the SMTP server.
		s = smtplib.SMTP(SMTP_SERVER)
		s.sendmail(SENDING_EMAIL_ADDR, ToEmail, msg.as_string())
		s.quit()				
	
def checkout_listener(dbapi_con, con_record, con_proxy):
	"""
	Unfortunately, MySQL can time out in Python. There isnt any elegant fox for the run of the mill connection, but there is a fix using the connection engine (which
	is obtained by using the function GetConnectionEngine()). It creates an event listener that listens for a disconnect, then automatically re-connects.
	It must be noted that you MUST set up the listener in your code after you get the connection engine; do this in this line of code after yo uget it:
			from sqlalchemy import event
			event.listen(NAME_OF_DB_ENGINE_VARIABLE, 'checkout', checkout_listener)
	this was fond on the website: https://stackoverflow.com/questions/18054224/python-sqlalchemy-mysql-server-has-gone-away
	"""
	
	try:
		try:
			dbapi_con.ping(False)
		except TypeError:
			dbapi_con.ping()
	except dbapi_con.OperationalError as exc:
		if exc.args[0] in (2006, 2013, 2014, 2045, 2055):
			
			raise sqlalchemy.exc.DisconnectionError()
		else:
			raise
		


					
		
		
		
		
		