#!/usr/bin/anaconda/python2.7/bin/python
import datetime
import os
import subprocess
import re

import pwd
import grp

import pandas as pd
import numpy as np

import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText

"""
This file houses basic functions that are helpful for maintaining and moving files.

This class can utilize a few environment files (if they exist):
SENDING_EMAIL_ADDR- The email address used to send mail.
ADMIN_EMAIL_ADDRS - a comma separated list of admin email addresses; if there is a problem these are email addresses that will be contacted.
ADMIN_TEXT_ADDRS - a comma separated list of phone text addresses; if there is a problem these are text messages that will be sent out.
SendEmail_ERROR_NOTIFICATION - If this is (1) email error notifications will be sent.
SMTP_SERVER - The SMTP server address you use for mail.
"""

def SendEmailWithBasicAttachment(fileToSend, To=None, Subject = '', Body = None, IsHTML=0, Preamble = ''):
	"""
	This function sends a basic file with a possible message; if the message was sent it returns True, otherwise False
	"""
	success = True
	
	SENDING_EMAIL_ADDR = os.environ['SENDING_EMAIL_ADDR']
	ADMIN_EMAIL_ADDRS = os.environ['ADMIN_EMAIL_ADDRS']
	SMTP_SERVER = os.environ['SMTP_SERVER']
	
	if (To is None):
		To = ADMIN_EMAIL_ADDRS
	
	msg = MIMEMultipart()
	msg['Subject'] = Subject
	msg['From'] = SENDING_EMAIL_ADDR
	msg['To'] = To
	msg.preamble = Preamble
	
	try:
		if ((Body != '') & (Body is not None)):
			if IsHTML==1:
				SentBody = MIMEText(Body, 'html')
			else:
				SentBody = MIMEText(Body, 'plain')		
		
			msg.attach(SentBody)	

		ctype, encoding = mimetypes.guess_type(fileToSend)
		if ctype is None or encoding is not None:
			ctype = "application/octet-stream"

		maintype, subtype = ctype.split("/", 1)

		if maintype == "text":
			fp = open(fileToSend)
			# Note: we should handle calculating the charset
			attachment = MIMEText(fp.read(), _subtype=subtype)
			fp.close()
		elif maintype == "image":
			fp = open(fileToSend, "rb")
			attachment = MIMEImage(fp.read(), _subtype=subtype)
			fp.close()
		elif maintype == "audio":
			fp = open(fileToSend, "rb")
			attachment = MIMEAudio(fp.read(), _subtype=subtype)
			fp.close()
		else:
			fp = open(fileToSend, "rb")
			attachment = MIMEBase(maintype, subtype)
			attachment.set_payload(fp.read())
			fp.close()
		encoders.encode_base64(attachment)

		attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
		msg.attach(attachment)

		s = smtplib.SMTP(SMTP_SERVER)
		s.sendmail(SENDING_EMAIL_ADDR, msg['To'], msg.as_string())
		s.quit()
	except:
		success = False
	
	return success

def CheckIfFileExists(localDirectory, localFileName):
	"""
	This takes as input the directory and file name (separately). The localDirectory should end in '/'.
	
	If the file exists, return True - False otehrwise.
	"""
	success = False
	if localDirectory[(len(localDirectory)-1):] != '/': localDirectory = localDirectory + '/'
	if os.path.isfile(localDirectory + localFileName): success = True
	return success	

def CheckIfRemoteFileExists(userName, hostName, fileDirAndName):
	"""
	This sub simply checks to see if a remote file exists; 	This returns True if it can find the file, and False if it cannot
	Note for this to work you MUST have a proper ssh key set up for the given user.
	"""
	success = False

	try:
		successInt = subprocess.check_output(['ssh','-q', '-o', 'StrictHostKeyChecking=no', userName + '@' + hostName, 'test', '-s', fileDirAndName, '&&', 'echo', '1', '||', 'echo', '0'])
		successInt = int(re.sub(r"\s", '', success, re.IGNORECASE))#strip out all whitespace - particularly the end crlf; then make sure its an int
		if(successInt==1): success = True
	except:
		success = False
		
	return success

def GetRemoteFile(userName, hostName, foreignFileDir, foreignFileName, localDirectory, localFileName = None):
	"""
	This sub gets a remote file from a server that supports ssh
	Note for this to work you MUST have a proper ssh key set up for the given user on the given server.
	
	If the file was downloaded, True is returned; otherwise, False is returned if the download failed.
	
	Note that this function uses CheckIfRemoteFileExists() - so there is no reason to use that otherwise as its just double-dipping. 
	"""
	
	#make sure bothforeignFileDir and localDirectory have a / at the end
	if foreignFileDir[(len(foreignFileDir)-1):] != '/': foreignFileDir = foreignFileDir + '/'
	if localDirectory[(len(localDirectory)-1):] != '/': localDirectory = localDirectory + '/'

	if (localFileName is None): localFileName = foreignFileName#if there is no local file name, use the foreign file name
		
	success = False
		
	if (CheckIfRemoteFileExists(userName, hostName, foreignFileDir + foreignFileName)):
		try:
			#download the file to the given local directory
			subprocess.check_output(['scp','-qpo', 'StrictHostKeyChecking=no', userName + '@' + hostName + ":" + foreignFileDir + foreignFileName, localDirectory + localFileName])
			
			success = CheckIfFileExists(localDirectory, localFileName)#check to see if the file was downloaded locally			
		except:
			success = False


	return success


def UploadFileToRemoteHost(userName, hostName, localDirectory, localFileName, foreignFileDir, foreignFileName = None):
	"""
	This sub uploads a file on a remote server that supports ssh
	Note for this to work you MUST have a proper ssh key set up for the given user on the given server.
	
	If the file was stored on the remote server, True is returned; otherwise, False is returned if the upload failed.
	
	Note that this function uses CheckIfRemoteFileExists() after the upload - so there is no reason to use that otherwise as its just double-dipping. 
	"""
	
	#make sure bothforeignFileDir and localDirectory have a / at the end
	if foreignFileDir[(len(foreignFileDir)-1):] != '/': foreignFileDir = foreignFileDir + '/'
	if localDirectory[(len(localDirectory)-1):] != '/': localDirectory = localDirectory + '/'
	
	if (foreignFileName is None): foreignFileName = localFileName
		
	success = False
	
	try:		
		success = CheckIfFileExists(localDirectory, localFileName)#check if the local file actually exists
			
		if (success):	
			#download the file to the given local directory
			subprocess.check_output(['scp','-qpo', 'StrictHostKeyChecking=no', localDirectory + localFileName, userName + '@' + hostName + ":" + foreignFileDir + foreignFileName])
			
			#check to see if the file was uploaded
			success = CheckIfRemoteFileExists(userName, hostName, foreignFileDir + foreignFileName)
			
	except:
		success = False


	return success
	

def GetFileWalkInformation(baseDirectory, getUID = False, getUserName = False, getGID = False, getGroupName = False, getAccessTime = False, getModifiedTime = False, get_md5sum = False, get_sha1sum = False, get_sha256sum = False, get_sha512sum = False, followLinks = False, printProgressToScreen = False):
	"""
	This function performs a 'walk' on a directory; this means you provide a directory, and this function will go find all files in all subdirectories along with some 
	specified attributes. 
	
	Returns: If there are files in the subdirectories, this will return a Pandas Dataframe detailing the information of those files; if no files are found, this 
		will return None.
	
	The variables are:
	baseDirectory (required) - The directory you wish to use as a base to find all files.
	getUID (default = False) - If you wish to collect the ID of the owner of the file, set this to True.
	getUserName (default = False) - If you wish to collect the user login of the owner of the file, set this to True.
	getGID (default = False) - If you wish to collect the group ID of the file, set this to True.
	getGroupName (default = False) - If you wish to collect the group name of the file, set this to True.
	getAccessTime (default = False) - If you wish to collect the last access time of the file, set this to True.
	getModifiedTime (default = False) -  If you wish to collect the last modified time of the file, set this to True.
	get_md5sum (default = False) - If you wish to collect the 128-bit MD5 hash of the file, set this to True.
	get_sha1sum (default = False) - If you wish to collect the SHA-1 hash of the file, set this to True.
	get_sha256sum (default = False) - If you wish to collect the SHA256 (256-bit) hash of the file, set this to True.
	get_sha512sum (default = False) - If you wish to collect the SHA512 (512-bit) hash of the file, set this to True.
	followLinks (default = False) - If you wish to follow symlinks, set this to True. 
	printProgressToScreen (default = False) - If you want to print the progress to the screen, set this to True; False otherwise.
	
	The base attributes are: 
	* filePath - The path of the file.
	* fileName - The file name (excluding the extension and dot)
	* fileExtension - The file extension (including the dot).
	* fileSize - The file suze (in bytes).
	
	There are additional parameters collected too, if you wish to collect them:
	* UID - The ID of the owner of the file.
	* userName - The user login of the owner of the file.
	* GID - The group ID of the file.
	* groupName - The group name of the file.
	* accessTime - The last access time of the file.
	* modifiedTime - The last modified time of the file.
	* md5sum - calculates the 128-bit MD5 hash of the file.
	* sha1sum - calculates the SHA-1 hash of the file.
	* sha256sum - calculates the SHA256 (256-bit) hash of the file.
	* sha512sum - calculates the SHA512 (512-bit) hash of the file.
	"""
	
	#make sure all of the passed variables are the type they should be - if any of them runs afoul just set to the default
	if(type(getUID).__name__ != 'bool'): getUID = False
	if(type(getUserName).__name__ != 'bool'): getUserName = False
	if(type(getGID).__name__ != 'bool'): getGID = False
	if(type(getGroupName).__name__ != 'bool'): getGroupName = False
	if(type(getAccessTime).__name__ != 'bool'): getAccessTime = False
	if(type(getModifiedTime).__name__ != 'bool'): getModifiedTime = False
	if(type(get_md5sum).__name__ != 'bool'): get_md5sum = False
	if(type(get_sha1sum).__name__ != 'bool'): get_sha1sum = False
	if(type(get_sha256sum).__name__ != 'bool'): get_sha256sum = False
	if(type(get_sha512sum).__name__ != 'bool'): get_sha512sum = False
	if(type(followLinks).__name__ != 'bool'): followLinks = False
	if(type(printProgressToScreen).__name__ != 'bool'): printProgressToScreen = False
	
	fileList = []

	#if the directory does not exist, tell the user and return None	
	if(os.path.exists(baseDirectory) == False):
		PrintTimestampedMsg(printProgressToScreen, "This directory does not exist (so we are skipping file discovery): {}".format(baseDirectory))
		return None
		

	#start the walk
	for (dirpath, dirnames, files) in os.walk(baseDirectory, followlinks = followLinks):
		#files is a list of filenames in dirpath
		individualFileList = []
		
		if((os.path.islink(dirpath)) & (followLinks == False)):
			PrintTimestampedMsg(printProgressToScreen, "This directory is a symlink and will be ignored (as specified): {}".format(dirpath))
		else:
		
			for fileName in files:
			#cycle through each file individually
				if ((os.path.isfile(dirpath + '/' + fileName) == False)): 
					#if os.path.islink(path)
					PrintTimestampedMsg(printProgressToScreen, "This is not a file (or it is a broken link) and will be ignored: {}".format(dirpath + '/' + fileName))
				else:
				
					#split the file into fileName and extension (with the dot going to the extension)
					fileAndExtension = os.path.splitext(fileName)
					
					#get most other attributes		
					(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(dirpath + '/' + fileName)
			
					#start the base list for this file
					individualFileList = [dirpath, fileAndExtension[0], fileAndExtension[1], size]
					
					#append any additional information if it was requested
					if getUID: individualFileList.append(uid)
					if getGID: individualFileList.append(gid)
					if getUserName: individualFileList.append(pwd.getpwuid(uid).pw_name)
					if getGroupName: individualFileList.append(grp.getgrgid(gid).gr_name) 
					if getAccessTime: individualFileList.append(datetime.datetime.utcfromtimestamp(atime).strftime('%Y-%m-%d %H:%M:%S'))
					if getModifiedTime:  individualFileList.append(datetime.datetime.utcfromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S'))	
					
					try:
						if get_md5sum: individualFileList.append(subprocess.check_output(["md5sum", dirpath + "/" + fileName]).split(' ')[0])
					except:
						get_md5sum = False
						PrintTimestampedMsg(printProgressToScreen, "The command 'md5sum' either does not work or is not installed...")
					
					try:
						if get_sha1sum: individualFileList.append(subprocess.check_output(["sha1sum", dirpath + "/" + fileName]).split(' ')[0])
					except:
						get_sha1sum = False
						PrintTimestampedMsg(printProgressToScreen, "The command 'sha1sum' either does not work or is not installed...")
						
					try:
						if get_sha256sum: individualFileList.append(subprocess.check_output(["sha256sum", dirpath + "/" + fileName]).split(' ')[0])
					except:
						get_sha256sum = False
						PrintTimestampedMsg(printProgressToScreen, "The command 'sha256sum' either does not work or is not installed...")
					
					try:				
						if get_sha512sum: individualFileList.append(subprocess.check_output(["sha512sum", dirpath + "/" + fileName]).split(' ')[0])
					except:
						get_sha512sum = False
						PrintTimestampedMsg(printProgressToScreen, "The command 'sha512sum' either does not work or is not installed...")
						
					#append the file to the fileList
					fileList.append(individualFileList)

	#Now we have to build the pandas dataframe - start with the base column names
	myColumns=['filePath', 'fileName', 'fileExtension', 'fileSize']

	#add on each column name if it was requested - this MUST be in the same order as ot was added to individualFileList!
	if getUID: myColumns.append("UID")
	if getGID: myColumns.append("userName")
	if getUserName: myColumns.append("GID")
	if getGroupName: myColumns.append("groupName")
	if getAccessTime: myColumns.append("accessTime")
	if getModifiedTime: myColumns.append("modifiedTime")
	if get_md5sum: myColumns.append("md5sum")
	if get_sha1sum: myColumns.append("sha1sum")
	if get_sha256sum: myColumns.append("sha256sum")
	if get_sha512sum: myColumns.append("sha512sum")
	
	#create and return the dataframe
	returnDF = pd.DataFrame(fileList, columns=myColumns)
	
	if (returnDF.shape[0] == 0):
		#if no data is returned, return None
		return None
	else:
		return returnDF
	
	
def GetFileWalkInformation2(baseDirectory, getUID = False, getUserName = False, getGID = False, getGroupName = False, getAccessTime = False, getModifiedTime = False, get_md5sum = False, get_sha1sum = False, get_sha256sum = False, get_sha512sum = False, followLinks = False, printProgressToScreen = False):
	"""
	This function performs a 'walk' on a directory; this means you provide a directory, and this function will go find all files in all subdirectories along with some 
	specified attributes. 
	
	Returns: If there are files in the subdirectories, this will return a Pandas Dataframe detailing the information of those files; if no files are found, this 
		will return None.
	
	The variables are:
	baseDirectory (required) - The directory you wish to use as a base to find all files.
	getUID (default = False) - If you wish to collect the ID of the owner of the file, set this to True.
	getUserName (default = False) - If you wish to collect the user login of the owner of the file, set this to True.
	getGID (default = False) - If you wish to collect the group ID of the file, set this to True.
	getGroupName (default = False) - If you wish to collect the group name of the file, set this to True.
	getAccessTime (default = False) - If you wish to collect the last access time of the file, set this to True.
	getModifiedTime (default = False) -  If you wish to collect the last modified time of the file, set this to True.
	get_md5sum (default = False) - If you wish to collect the 128-bit MD5 hash of the file, set this to True.
	get_sha1sum (default = False) - If you wish to collect the SHA-1 hash of the file, set this to True.
	get_sha256sum (default = False) - If you wish to collect the SHA256 (256-bit) hash of the file, set this to True.
	get_sha512sum (default = False) - If you wish to collect the SHA512 (512-bit) hash of the file, set this to True.
	followLinks (default = False) - If you wish to follow symlinks, set this to True. 
	printProgressToScreen (default = False) - If you want to print the progress to the screen, set this to True; False otherwise.
	
	The base attributes are: 
	* filePath - The path of the file.
	* fileName - The file name (excluding the extension and dot)
	* fileExtension - The file extension (including the dot).
	* fileSize - The file suze (in bytes).
	
	There are additional parameters collected too, if you wish to collect them:
	* UID - The ID of the owner of the file.
	* userName - The user login of the owner of the file.
	* GID - The group ID of the file.
	* groupName - The group name of the file.
	* accessTime - The last access time of the file.
	* modifiedTime - The last modified time of the file.
	* md5sum - calculates the 128-bit MD5 hash of the file.
	* sha1sum - calculates the SHA-1 hash of the file.
	* sha256sum - calculates the SHA256 (256-bit) hash of the file.
	* sha512sum - calculates the SHA512 (512-bit) hash of the file.
	"""
	
	#make sure all of the passed variables are the type they should be - if any of them runs afoul just set to the default
	if(type(getUID).__name__ != 'bool'): getUID = False
	if(type(getUserName).__name__ != 'bool'): getUserName = False
	if(type(getGID).__name__ != 'bool'): getGID = False
	if(type(getGroupName).__name__ != 'bool'): getGroupName = False
	if(type(getAccessTime).__name__ != 'bool'): getAccessTime = False
	if(type(getModifiedTime).__name__ != 'bool'): getModifiedTime = False
	if(type(get_md5sum).__name__ != 'bool'): get_md5sum = False
	if(type(get_sha1sum).__name__ != 'bool'): get_sha1sum = False
	if(type(get_sha256sum).__name__ != 'bool'): get_sha256sum = False
	if(type(get_sha512sum).__name__ != 'bool'): get_sha512sum = False
	if(type(followLinks).__name__ != 'bool'): followLinks = False
	if(type(printProgressToScreen).__name__ != 'bool'): printProgressToScreen = False
	
	fileList = []

	#if the directory does not exist, tell the user and return None	
	if(os.path.exists(baseDirectory) == False):
		PrintTimestampedMsg(printProgressToScreen, "This directory does not exist (so we are skipping file discovery): {}".format(baseDirectory))
		return None
		

	#start the walk
	#https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-list-of-lists
	multipleFiles = [[[dirpath]*len(files), files] for (dirpath, dirnames, files) in os.walk(baseDirectory, followlinks = followLinks)]
	fileList = [listedFile for representedDirectory in multipleFiles for listedFile in representedDirectory[1]]
	dirList = [listedDirectory for representedDirectory in multipleFiles for listedDirectory in representedDirectory[0]]
	
	fileNoExtension = [os.path.splitext(file)[0] for file in fileList]
	fileExtension = [os.path.splitext(file)[1] for file in fileList]	
	
	fileExists = [os.path.isfile(dir + '/' + file) for dir, file in zip(dirList, fileList)]
	
	filesNP = np.empty([len(dirList),3],dtype=object)
	filesNP[:, 0] = dirList
	filesNP[:, 1] = fileNoExtension	
	filesNP[:, 2] = fileExtension	
	
	filesNP = filesNP[fileExists]
	
	dirAndFile = filesNP[:, 0] + '/' + filesNP[:, 1] + filesNP[:, 2]
	fileStats = [os.stat(file)[4:9] for file in dirAndFile]	
	#fileStats = [os.stat(dir + '/' + file)[4:9] for dir, file in zip(filesNP[:, 0], filesNP[:, 1] + filesNP[:, 2])]

	sizes = [file[2] for file in fileStats]

	filesNP = np.column_stack([filesNP, sizes])
	
	if (getUserName | getUID): 
		uids = [file[0] for file in fileStats]
		if getUID: filesNP = np.column_stack([filesNP, uids])
		if getUserName: 
			userNames = [pwd.getpwuid(uid).pw_name for uid in uids]
			filesNP = np.column_stack([filesNP, userNames])	
		
		
	if (getGroupName | getGID):  
		gids = [file[1] for file in fileStats]
		if getGID: filesNP = np.column_stack([filesNP, gids])
		if getUserName:
			groupNames = [grp.getgrgid(gid).gr_name for gid in gids]
			filesNP = np.column_stack([filesNP, groupNames])
		
		 
	if getAccessTime: 
		accessTimes = [datetime.datetime.utcfromtimestamp(file[3]).strftime('%Y-%m-%d %H:%M:%S') for file in fileStats]	
		filesNP = np.column_stack([filesNP, accessTimes])
	if getModifiedTime:  		
		modifiedTimes = [datetime.datetime.utcfromtimestamp(file[4]).strftime('%Y-%m-%d %H:%M:%S') for file in fileStats]
		filesNP = np.column_stack([filesNP, modifiedTimes])		

	try:
		if get_md5sum: 
			md5sum = [subprocess.check_output(["md5sum", file]).split(' ')[0] for file in dirAndFile]
			filesNP = np.column_stack([filesNP, md5sum])		
	except:
		get_md5sum = False
		PrintTimestampedMsg(printProgressToScreen, "The command 'md5sum' either does not work or is not installed...")
	
	try:
		if get_sha1sum: 
			sha1sum = [subprocess.check_output(["sha1sum", file]).split(' ')[0] for file in dirAndFile]
			filesNP = np.column_stack([filesNP, sha1sum])			
	except:
		get_sha1sum = False
		PrintTimestampedMsg(printProgressToScreen, "The command 'sha1sum' either does not work or is not installed...")
		
	try:
		if get_sha256sum: 
			sha256sum = [subprocess.check_output(["sha256sum", file]).split(' ')[0] for file in dirAndFile]
			filesNP = np.column_stack([filesNP, sha256sum])			
	except:
		get_sha256sum = False
		PrintTimestampedMsg(printProgressToScreen, "The command 'sha256sum' either does not work or is not installed...")
	
	try:				
		if get_sha512sum: 
			sha512sum = [subprocess.check_output(["sha512sum", file]).split(' ')[0] for file in dirAndFile]
			filesNP = np.column_stack([filesNP, sha512sum])						
	except:
		get_sha512sum = False
		PrintTimestampedMsg(printProgressToScreen, "The command 'sha512sum' either does not work or is not installed...")

	#Now we have to build the pandas dataframe - start with the base column names
	myColumns=['filePath', 'fileName', 'fileExtension', 'fileSize']

	#add on each column name if it was requested - this MUST be in the same order as ot was added to individualFileList!
	if getUID: myColumns.append("UID")
	if getGID: myColumns.append("userName")
	if getUserName: myColumns.append("GID")
	if getGroupName: myColumns.append("groupName")
	if getAccessTime: myColumns.append("accessTime")
	if getModifiedTime: myColumns.append("modifiedTime")
	if get_md5sum: myColumns.append("md5sum")
	if get_sha1sum: myColumns.append("sha1sum")
	if get_sha256sum: myColumns.append("sha256sum")
	if get_sha512sum: myColumns.append("sha512sum")
	
	#create and return the dataframe
	returnDF = pd.DataFrame(filesNP, columns=myColumns)
	
	if (returnDF.shape[0] == 0):
		#if no data is returned, return None
		return None
	else:
		return returnDF


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

		

