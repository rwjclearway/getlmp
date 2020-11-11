import os
import time
import json
import datetime
import string


def WriteLogOut(sStr):
    sStr = str(sStr)
    dttime = datetime.datetime.now()
    sFileTime = dttime.strftime("%Y-%m-%d")
        
    sOutString = dttime.strftime("%Y-%m-%dT%H:%M:%S") 
    sFileName = 'LMPLOG ' + sFileTime +'.log'
    dtslastruntime = dttime.timestamp()
    #set the data the first time we run (defaults ) 
    


    sLogPath = 'C:\\ADS_PY_APP\\LOG\\'
    sFileAndPath = sLogPath + sFileName
    try:
        sOutString  = sOutString + ' ' + sStr
    except exception as e:
        sOutString = sOutString + ' ' + e
    configfileexist = os.path.isfile(sFileName) 
    if not configfileexist: 
        with open(sFileAndPath, 'a') as outfile:  
            outfile.write(sOutString +'\n')
    else:
        with open(sFileAndPath, 'a') as outfile:  
            outfile.write(sOutString +'\n')
    print(sOutString)