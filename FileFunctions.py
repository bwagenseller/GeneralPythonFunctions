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
import shutil

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

def RunCommand(command):
	"""
	This function simply runs a command that may fail (and a failure is somewhat expected); True is returned if successful, false otehrwise.
	
	Unfortunately nested try/except blocks get a bit screwy (as in, if the inner block throws an exception it will trigger the exception in _both_ blocks)
	We can get around this if we make this its own function.
	
	"""
	
	retVal = False
	try:
		retVal = subprocess.check_output(command, shell=True, close_fds=True, stderr=None, stdin=None)
		#, stdout = None
		
	#unfortunately, the above can vary in what is returned - if we are at this point we did not bomb out, so the command was run. Return 'true'
		retVal = True
	except: 
		retVal = False

	return retVal


def RunCommandWithResults(command):
	"""
	Similar to RunCommand() above, but will return the results if successful; if failed, this will return None.
	
	Unfortunately nested try/except blocks get a bit screwy (as in, if the inner block throws an exception it will trigger the exception in _both_ blocks)
	We can get around this if we make this its own function.
	
	"""
	
	retVal = None
	try:
		retVal = subprocess.check_output(command, shell=True, close_fds=True, stderr=None, stdin=None)
	#, stdout = None
		
	except: 
		retVal = None

	return retVal


def CheckIfRemoteFileExists(userName, hostName, fileDirAndName):
	"""
	This sub simply checks to see if a remote file exists; 	This returns True if it can find the file, and False if it cannot
	Note for this to work you MUST have a proper ssh key set up for the given user.
	"""
	success = False

	try:
		successInt = subprocess.check_output(['ssh','-q', '-o', 'StrictHostKeyChecking=no', userName + '@' + hostName, 'test', '-s', fileDirAndName, '&&', 'echo', '1', '||', 'echo', '0'])
		successInt = int(re.sub(r"\s", '', successInt, re.IGNORECASE))#strip out all whitespace - particularly the end crlf; then make sure its an int
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
	

def GetFileWalkInformation(baseDirectory, getUID = False, getUserName = False, getGID = False, getGroupName = False, getAccessTime = False, getModifiedTime = False, get_md5sum = False, get_sha1sum = False, get_sha256sum = False, get_sha512sum = False, followLinks = False, printProgressToScreen = False, useUTC = False):
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
	useUTC (default = False) - If you want to display UTC time in messages, set this to True; if you want to see time in messages as local time, set to False.
	
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
		PrintTimestampedMsg(printProgressToScreen, "This directory does not exist (so we are skipping file discovery): {}".format(baseDirectory), useUTC = useUTC)
		return None
		

	#start the walk
	for (dirpath, dirnames, files) in os.walk(baseDirectory, followlinks = followLinks):
		#files is a list of filenames in dirpath
		individualFileList = []
		
		if((os.path.islink(dirpath)) & (followLinks == False)):
			PrintTimestampedMsg(printProgressToScreen, "This directory is a symlink and will be ignored (as specified): {}".format(dirpath), useUTC = useUTC)
		else:
		
			for fileName in files:
			#cycle through each file individually
				if ((os.path.isfile(dirpath + '/' + fileName) == False)): 
					#if os.path.islink(path)
					PrintTimestampedMsg(printProgressToScreen, "This is not a file (or it is a broken link) and will be ignored: {}".format(dirpath + '/' + fileName), useUTC = useUTC)
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
						PrintTimestampedMsg(printProgressToScreen, "The command 'md5sum' either does not work or is not installed...", useUTC = useUTC)
					
					try:
						if get_sha1sum: individualFileList.append(subprocess.check_output(["sha1sum", dirpath + "/" + fileName]).split(' ')[0])
					except:
						get_sha1sum = False
						PrintTimestampedMsg(printProgressToScreen, "The command 'sha1sum' either does not work or is not installed...", useUTC = useUTC)
						
					try:
						if get_sha256sum: individualFileList.append(subprocess.check_output(["sha256sum", dirpath + "/" + fileName]).split(' ')[0])
					except:
						get_sha256sum = False
						PrintTimestampedMsg(printProgressToScreen, "The command 'sha256sum' either does not work or is not installed...", useUTC = useUTC)
					
					try:				
						if get_sha512sum: individualFileList.append(subprocess.check_output(["sha512sum", dirpath + "/" + fileName]).split(' ')[0])
					except:
						get_sha512sum = False
						PrintTimestampedMsg(printProgressToScreen, "The command 'sha512sum' either does not work or is not installed...", useUTC = useUTC)
						
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
	
"""
# At one point I tried to pseudo-vectorize 'GetFileWalkInformation()'; unfortunately, most of the file-oriented commands are not vectorized (that I could find) and I wasn't 
# happy with that, so I tried replacing the giant 'for' loop with the below - it worked, but it was 2-3x slower (I think the multiple 'for' loops killed it)
# If I eventually figure out some library that is vectorized while also file-friendly I will use that instead (or if I write one myself), but for now
# this code sits here, only to capture what I tried
	#start the walk
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
"""	

def	__compressFiles(mySeries, baseDirectory, targetDirectory, deleteIfZipped, zipType = 0):
	#
	"""
	#######################################################################################################
	This function is only meant to be used by CompressAllInDirectory() and is NOT meant for normal public use!
	#######################################################################################################
	
	This function takes a series that contains 3 items, all elements of a file: filePath (no trailing '/'), fileName (with no extension or dot at the end), fileExtension (including dot)
	If performs the work as specified in CompressAllInDirectory()
	"""	
	
	#if the targetDirectory is not None and if targetDirectory != baseDirectory, copy the file to the new location
	if ((targetDirectory is not None) & (targetDirectory != baseDirectory)):
		newLocation = mySeries[0].replace(baseDirectory, targetDirectory) #replace the base directory structure with the target structure
		RunCommand("mkdir -p " + '"' + newLocation + '"')
		
		shutil.copyfile(mySeries[0] + '/' + mySeries[1] + mySeries[2], newLocation + '/' + mySeries[1] + mySeries[2])
	else:
		newLocation = mySeries[0]
		
	#change to the new location
	os.chdir(newLocation)
	
	oldFileNameWithQuotes = '"' + mySeries[1] + mySeries[2] + '"'
	
	isAlreadyZipped = False
	
	if((mySeries[2] == '.zip') | (mySeries[2] == '.7z') | (mySeries[2] == '.gz')): isAlreadyZipped = True

	#if we wish to zip files , perform the command
	if((zipType == 0) & (isAlreadyZipped == False)): 
		RunCommand("zip \"" + mySeries[1] + ".zip\" " + oldFileNameWithQuotes)
		#new files end in .zip and lop off old names

	#if we wish to 7z files, perform the command
	if((zipType == 1) & (isAlreadyZipped == False)):
		RunCommand("7z a \"" + mySeries[1] + ".7z\" " + oldFileNameWithQuotes)

	#if we wish to gzip files, perform the command
	if((zipType == 2) & (isAlreadyZipped == False)): 
		RunCommand("gzip " + oldFileNameWithQuotes)

	#if (the base and target directory is not none and the target and base directories are not the same AND compression of some sort was performed, delete the new unzipped file
	#Also, the zip type cannot equal 2 (gzip) as gzip natively deletes the file afterwards
	if ((targetDirectory is not None) & (targetDirectory != baseDirectory) & (zipType != 2) & (isAlreadyZipped == False)): os.remove(newLocation + '/' + mySeries[1] + mySeries[2])
	
	"""
	If we want to delete the original file AND the new file is approximated to be present, delete the original file. Why 'approximated'? we cannot guarantee the extension of the file 
		we unzipped (although the base name should be the same); that said, we can use a wildcard (with a dot at the end of the filename) to see if the new file is present. There is a 
		chance another file with the same file name exists that ends in a dot wildcard, but the chances are low. If this does not sit well with you, you can extract the zip to a clean 
		folder, examine all content names, and then move to the target; I avoided this as I did not think the operational cost was worth it, but if you want to implement it you can.
	""" 	
	#the '."*' is important
	#if ((FileFunc.RunCommandWithResults("ls -la " + '"' + newLocation + '/' + mySeries[1] + '."*') is not None) & (unZipPerformed) & (deleteIfUnzipped)): os.remove(mySeries[0] + '/' + mySeries[1] + mySeries[2])
	
def CompressAllInDirectory(baseDirectory, targetDirectory = None, deleteIfZipped = False, getZipped_sha256sum = True, zipType = 0, printProgressToScreen = True, useUTC = False):
	"""
	This function takes a directory and compresses (via unzip, gunzip, or 7z) all files in that base directory (As well as all subdirectories) and saves them to a target directory 
	(if one is supplied, otherwise the files are compressed and stored in the same directory). If the files are already compressed files, they are simply copied to the 
	new target directory.
	
	If the target directory is not specified, it is assumed that the original directory is the target directory and we are simply compressing all approved files we find.
	
	Note this method assumes one file per compression.
	
	The practical use for this function is if you have a number of mid to large files that you wish to compress and store for later use. 
	
	Tarballs are not supported.  
	
	Parameters:
		baseDirectory - The folder (and all subfolders) which you want to search for and decompress files.
		targetDirectory - If this is set, all compressed files are decompressed here (and all non-compressed files are simply copied here). If this is left None, baseDirectory is used.
		deleteIfZipped - If the file is compressed in the new location and this is set to True, the original file is deleted.
		getZipped_sha256sum - Set to True if you wish to get the sha256sum of the compressed file.
		zipType - Set to 0 (default) if you wish to store as a .zip; 1 if you wish to store as a .7z; 2 if you wish to store as a .gz.
		printProgressToScreen - If you wish to see diagnostic messages, set to True. 
		useUTC - If you wish to see diagnostic timestamps in UTC time, set to true (otherwise they are in local time).
		
	Output: Pandas dataframe, containing basic information (and the sha256sum if you opted to collect it) about all files compressed in the target directory.
		
	####################################################POTENTIAL GOTCHAS################################################################
	1. If the file is already compressed in one of the available formats, there will be no attempt to re-compress the file.
	
	##########################################THINGS I HAVE TO FIX IN THIS FUNCTION######################################################
	1. If two files have the same name and different extensions, only one of them will be saved as a compressed file.
	2. Do a better job of handling problematic characters in file names (!, ', etc)
	3. Currently, the original compressed file is not deleted.
	"""
	use7z = False
	useGz = False
	useZip = False
			
	if (zipType == 1):
		use7z = True
	elif (zipType == 2):
		useGz = True
	else:
		useZip = True
		
	if (useZip) & (RunCommandWithResults("which unzip") is None): 
		PrintTimestampedMsg(printProgressToScreen = printProgressToScreen, myMsg = "You opted to use zip, but it is not installed - exiting 'ZipAllInDirectory()'...")
		return None
	
	if (use7z) & (RunCommandWithResults("which 7z") is None):
		PrintTimestampedMsg(printProgressToScreen = printProgressToScreen, myMsg = "You opted to use 7zip, but it is not installed - exiting 'ZipAllInDirectory()'...")
		return None
		
	if (useGz) & (RunCommandWithResults("which gunzip") is None): 
		PrintTimestampedMsg(printProgressToScreen = printProgressToScreen, myMsg = "You opted to use Gzip, but it is not installed - exiting 'ZipAllInDirectory()'...")
		return None		
	
	unCompressedDF = GetFileWalkInformation(baseDirectory = baseDirectory, getAccessTime = True, getModifiedTime = True, followLinks = False, printProgressToScreen = printProgressToScreen, useUTC = useUTC)
	
	
	#this will actually do the compression
	unCompressedDF[['filePath', 'fileName', 'fileExtension']].apply(__compressFiles, args=(baseDirectory, targetDirectory, deleteIfZipped, zipType), axis = 1) 
	
	#now that the files have been compressed, get some file stats on the new files
	
	compressedDF = GetFileWalkInformation(baseDirectory = targetDirectory, getAccessTime = True, getModifiedTime = True, get_sha256sum = getZipped_sha256sum, followLinks = False, printProgressToScreen = printProgressToScreen, useUTC = useUTC)
	
	return compressedDF	

def	__decompressFiles(mySeries, baseDirectory, targetDirectory, deleteIfUnzipped, unzipZipFiles, unzip7zFiles, unzipGzFiles):
	"""
	#######################################################################################################
	This function is only meant to be used by DecompressAllInDirectory() and is NOT meant for normal public use!
	#######################################################################################################
	
	This function takes a series that contains 3 items, all elements of a file: filePath (no trailing '/'), fileName (with no extension or dot at the end), fileExtension (including dot)
	If performs the work as specified in DecompressAllInDirectory()
	"""	
	
	unZipPerformed = False
	
	#if the targetDirectory is not None and if targetDirectory != baseDirectory, copy the file to the new location
	if ((targetDirectory is not None) & (targetDirectory != baseDirectory)):
		newLocation = mySeries[0].replace(baseDirectory, targetDirectory) #replace the base directory structure with the target structure
		RunCommand("mkdir -p " + '"' + newLocation + '"')
		
		#RunCommand("cp " + '"' + mySeries[0] + '/' + mySeries[1] + mySeries[2] + ' "' + newLocation + '/' + mySeries[1] + mySeries[2] + '"')
		shutil.copyfile(mySeries[0] + '/' + mySeries[1] + mySeries[2], newLocation + '/' + mySeries[1] + mySeries[2])
	else:
		newLocation = mySeries[0]
		
	#change to the new location
	os.chdir(newLocation)
	
	newFileDirAndNameWithQuotes = '"' + newLocation + '/' + mySeries[1] + mySeries[2] + '"'

	
	#if we wish to unzip .zip files AND this is a .zip file, perform the command
	if((unzipZipFiles) & (mySeries[2] == '.zip')): 
		RunCommand("unzip -o " + newFileDirAndNameWithQuotes + " -d " + '"' + newLocation + '"')
		unZipPerformed = True
	
	#if we wish to unzip .7z files AND this is a .7z file, perform the command
	if((unzip7zFiles) & (mySeries[2] == '.7z')): 
		RunCommand("7z x " + newFileDirAndNameWithQuotes + " -aoa")
		unZipPerformed = True		
	
	#if we wish to unzip .gz files AND this is a .gz file AND its not actually a .tar.gz file (as we do not want to mess with tarballs), perform the command
	if((unzipGzFiles) & (mySeries[2] == '.gz') & (mySeries[1][(len(mySeries[1])-4):] != '.tar')): 
		RunCommand("gunzip " + newFileDirAndNameWithQuotes)	
		unZipPerformed = True

	#if (the base and target directory is not none and the target and base directories are not the same AND an unzip of some sort was performed, delete the new unzipped file
	if ((targetDirectory is not None) & (targetDirectory != baseDirectory) & (unZipPerformed)): os.remove(newLocation + '/' + mySeries[1] + mySeries[2])
	
	"""
	If we want to delete the original file AND the new file is approximated to be present, delete the original file. Why 'approximated'? we cannot guarantee the extension of the file 
		we unzipped (although the base name should be the same); that said, we can use a wildcard (with a dot at the end of the filename) to see if the new file is present. There is a 
		chance another file with the same file name exists that ends in a dot wildcard, but the chances are low. If this does not sit well with you, you can extract the zip to a clean 
		folder, examine all content names, and then move to the target; I avoided this as I did not think the operational cost was worth it, but if you want to implement it you can.
	""" 	
	#the '."*' is important
	if ((RunCommandWithResults("ls -la " + '"' + newLocation + '/' + mySeries[1] + '."*') is not None) & (unZipPerformed) & (deleteIfUnzipped)): os.remove(mySeries[0] + '/' + mySeries[1] + mySeries[2])

def DecompressAllInDirectory(baseDirectory, targetDirectory = None, deleteIfUnzipped = False, getUnzipped_sha256sum = True, unzipZipFiles = True, unzip7zFiles = True, unzipGzFiles = True, printProgressToScreen = False, useUTC = False):
	"""
	This function takes a directory and decompresses (via unzip, gunzip, and 7z) all files in that base directory (As well as all subdirectories) to a target directory (if one is supplied, 
	otherwise the files are decompressed in the same directory). If the files are not compressed files (or, if they are not in an approved compressed format), they are simply copied to the 
	new target directory.
	
	If the target directory is not specified, it is assumed that the original directory is the target directory and we are simply de-compressing all approved files we find.
	
	Note this method only works on compressed files that hold exactly ONE file and the contained file has the exact name as the zipped file (with the exception being the extension).
	
	The practical use for this function is if you have a number of mid to large files that are all individually compressed and want to decompress all of them. 
	
	Tarballs are not supported.  
	
	Parameters:
		baseDirectory - The folder (and all subfolders) which you want to search for and decompress files.
		targetDirectory - If this is set, all compressed files are decompressed here (and all non-compressed files are simply copied here). If this is left None, baseDirectory is used.
		deleteIfUnzipped - If the file is decompressed in the new location and this is set to True, the original compressed file is deleted.
		getUnzipped_sha256sum - Set to True if you wish to get the sha256sum of the unzipped file.
		unzipZipFiles - If set to True, decompresses .zip files.
		unzip7zFiles - If set to True, decompresses .7z files.
		unzipGzFiles - If set to True, decompresses .gz (but not tar.gz) files.
		printProgressToScreen - If you wish to see diagnostic messages, set to True. 
		useUTC - If you wish to see diagnostic timestamps in UTC time, set to true (otherwise they are in local time).
		
	Output: Pandas dataframe, containing basic information (and the sha256sum if you opted to collect it) about all files in the target directory.
	"""
	
	if (unzipZipFiles) & (RunCommandWithResults("which unzip") is None): unzipZipFiles = False
	if (unzip7zFiles) & (RunCommandWithResults("which 7z") is None): unzip7zFiles = False
	if (unzipGzFiles) & (RunCommandWithResults("which gunzip") is None): unzipGzFiles = False 
	
	#if the target directory is not set, set it to the base directory
	if (targetDirectory is None): targetDirectory = baseDirectory
	
	if (not (unzipZipFiles | unzip7zFiles | unzipGzFiles)): 
		print "No compression tools installed (unzip / 7z / gunzip) or the ones that are installed are not selected - exiting method..."
		return None

	
	compressedDF = GetFileWalkInformation(baseDirectory = baseDirectory, getAccessTime = True, getModifiedTime = True, followLinks = False, printProgressToScreen = printProgressToScreen, useUTC = useUTC)
	
	#this will actually do the decompression
	compressedDF[['filePath', 'fileName', 'fileExtension']].apply(__decompressFiles, args=(baseDirectory, targetDirectory, deleteIfUnzipped, unzipZipFiles, unzip7zFiles, unzipGzFiles), axis = 1) 
	
	#now that the files have been decompressed, get some file stats on the new files
	
	deCompressedDF = GetFileWalkInformation(baseDirectory = targetDirectory, getAccessTime = True, getModifiedTime = True, get_sha256sum = getUnzipped_sha256sum, followLinks = False, printProgressToScreen = printProgressToScreen, useUTC = useUTC)
	
	return deCompressedDF


def PrintTimestampedMsg(printProgressToScreen = False, myMsg = "NULL", useUTC = False, printToFile = None):
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

		

