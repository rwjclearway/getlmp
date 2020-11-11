# first read the most recent vlue in the directory 
import os
import time
import json
import datetime
import string
import shutil
import logging
import ftplib
import collections
import LMPFileLog
import uuid

def GetCurrentSiteRecordsWithLock():

    # Filename is SiteDataBase.json

    sRootPath = getPath()
    sFilePath = sRootPath + 'SiteDataBase.json'
    notopen = True
    uid = uuid.uuid4()
    sLockFileName = sRootPath + str(uid) +'.LOCK'
    while notopen :
        try:
            FileisReady = os.path.isfile(sFilePath) 
            if FileisReady:
                os.rename(sFilePath,sLockFileName)

                outfile = open(sLockFileName,'r')
                SiteRecords = json.load(outfile)
                outfile.close()
                notopen=False
            else:
                time.sleep(1)
                LMPFileLog.WriteLogOut('\n READ DB JSON LOCKED or MISSING  ')
        except IOError:
            LMPFileLog.WriteLogOut('\n READ DB JSON LOCKED')
            time.sleep(1)







     
    return SiteRecords, sLockFileName

def WriteToDBWithLock(Records,sLockFileName):


    # Filename is SiteDataBase.json
    sRootPath = getPath()
    sFilePath = sRootPath + 'SiteDataBase.json'
    notopen = True
    while notopen :
        try:
            FileisReady = os.path.isfile(sLockFileName) 
            if FileisReady:
                LMPFileLog.WriteLogOut('opening f ' + sLockFileName)
                outfile = open(sLockFileName, 'w')
                LMPFileLog.WriteLogOut('writing to f ' + sFilePath + ' to ' + sLockFileName)
                json.dump(Records, outfile )
                outfile.close()
                os.rename(sLockFileName,sFilePath)
                notopen=False
        except Exception as e:
            LMPFileLog.WriteLogOut('\n WRITE DB JSON LOCKED: '+ str(e) )
            time.sleep(1)
    return 1


def getPath():
    with open('lmpjobconfig.json') as json_file:  
        configdata = json.load(json_file)
    sPath = configdata['root']
    return sPath


def inSitelist(val, list):

    for site in list['Sites']:
        sname = site['Name']
        slmptag = site['LMPTag']
        sdotmwtag = site['DOTMWTag']
        ssuppmwtag = site['SUPPMWTag']
        if sname == val:
            return val
        if ssuppmwtag == val:  # ResourceID and Supp MW tag are the same , but we may use others later
            return val

    return ''


def GetCurrentSiteRecords():

    # Filename is SiteDataBase.json

    sRootPath = getPath()
    sFilePath = sRootPath + 'SiteDataBase.json'
    notopen = True
    sLockFileName = sRootPath + 'SiteDataBase.LOCK'
    while notopen :
        try:
            FileisReady = os.path.isfile(sFilePath) 
            if FileisReady:
                os.rename(sFilePath,sLockFileName)

                outfile = open(sLockFileName,'r')
                SiteRecords = json.load(outfile)
                outfile.close()
                os.rename(sLockFileName,sFilePath)
                notopen=False
        except IOError:
            LMPFileLog.WriteLogOut('\n READ DB JSON LOCKED')
            time.sleep(1)







     
    return SiteRecords

def CleanOutOldLMPRecords(SecondsFromStart):

    SiteRecords, lockName = GetCurrentSiteRecordsWithLock()
    RowsAdded = 0
    OutRecords = dict(
        Sites = [])
    for site in SiteRecords['Sites']:  ## this is how we walk through it 
        
        name = site['ResourceID']
        # only copy the current rows to the new list and save it 
        NewSite = dict(
            Name = site['Name'],
            LMPTag = site['LMPTag'],
            ResourceID = site['ResourceID'],
            OIRows = site['OIRows'],
            LMPRows = [],
            DispatchRows = site['DispatchRows']
            )

        #Get the date sorted out 


            ## 2020-09-24T19:37:30Z
        #if the time is passed remove the instrution 
        dCurrenttime = datetime.datetime.utcnow()
        dCurrenttime = dCurrenttime - datetime.timedelta(seconds=SecondsFromStart)
        for row in site['LMPRows']:
            if type(row) == dict :
                EndTime = row['EndTime']
                dRowEndTime = datetime.datetime.strptime(EndTime,"%d-%m-%YT%H:%M:%SZ")
                if dRowEndTime > dCurrenttime:  
                    NewSite['LMPRows'].append(row)

                    RowsAdded += 1

        OutRecords['Sites'].append(NewSite)

        dCurrenttime = datetime.datetime.now()
        #for row in site['IORows']:
        #    OpsInsEndTime = row['OpsInsEndTime']
        #    dRowEndTime = datetime.datetime.strptime(OpsInsEndTime,'%Y-%m-%dT%H:%M:%SZ')
        #    if dRowEndTime < dCurrenttime:                
        #        #Delete the Current Row
        #        site[name].remove(row)
        #        RowsRemoved += 1
    
    WriteToDBWithLock(OutRecords,lockName)
    return OutRecords

def CleanOutOldOIRecords(SecondsFromStart):

   
    SiteRecords, lockName = GetCurrentSiteRecordsWithLock()
    RowsAdded = 0
    OutRecords = dict(
        Sites = [])
    for site in SiteRecords['Sites']:  ## this is how we walk through it 
        
        name = site['ResourceID']
        # only copy the current rows to the new list and save it 
        NewSite = dict(
            Name = site['Name'],
            LMPTag = site['LMPTag'],
            ResourceID = site['ResourceID'],
            OIRows = [], ## Cleaning Out OperationInstruction  
            LMPRows = site['LMPRows'],
            DispatchRows = site['DispatchRows']  
            )

        #Get the date sorted out 
        #if the time is passed remove the instrution 
        dCurrenttime = datetime.datetime.utcnow()
        dCurrenttime = dCurrenttime - datetime.timedelta(seconds=SecondsFromStart)
        for row in site['OIRows']:
            if type(row) == object :
                EndTime = row['EndTime']
                dRowEndTime = datetime.datetime.strptime(EndTime,"%Y-%m-%dT%H:%M:00-00:00")
                if dRowEndTime > dCurrenttime:  
                    NewSite['OIRows'].append(row)

                    RowsAdded += 1

        OutRecords['Sites'].append(NewSite)

        dCurrenttime = datetime.datetime.now()

    WriteToDBWithLock(OutRecords,lockName)
    return OutRecords

def CleanOutOldDispatchRecords(SecondsFromStart):

    SiteRecords, lockName = GetCurrentSiteRecordsWithLock()
    RowsAdded = 0
    OutRecords = dict(
        Sites = [])
    for site in SiteRecords['Sites']:  ## this is how we walk through it 
        
        name = site['ResourceID']
        # only copy the current rows to the new list and save it 
        NewSite = dict(
            Name = site['Name'],
            LMPTag = site['LMPTag'],
            ResourceID = site['ResourceID'],
            OIRows = site['OIRows'],
            LMPRows = site['LMPRows'],
            DispatchRows = []  ## Cleaning out Dispatch 
            )

        #Get the date sorted out 
        #if the time is passed remove the instrution 
        dCurrenttime = datetime.datetime.utcnow()
        dCurrenttime = dCurrenttime - datetime.timedelta(seconds=SecondsFromStart)
        for row in site['DispatchRows']:
            if type(row) == object :
                EndTime = row['EndTime']
                dRowEndTime = datetime.datetime.strptime(EndTime,"%Y-%m-%dT%H:%M:00")
                if dRowEndTime > dCurrenttime:  
                    NewSite['DispatchRows'].append(row)

                    RowsAdded += 1

        OutRecords['Sites'].append(NewSite)

        dCurrenttime = datetime.datetime.now()

    WriteToDBWithLock(OutRecords,lockName)
    return OutRecords

## this will add any new sites if any have been added to site master ( only called once per execution ) 
def InitSiteDataModel(CurrentMasterSiteList):
    nNumAdded = 0
    SiteRecords, lockName = GetCurrentSiteRecordsWithLock()
    NewSiteList = [];
    for MasterSite in CurrentMasterSiteList['Sites']:
        bFound = False
        for site in SiteRecords['Sites']:  ## this is how we walk through it 
            name = site['ResourceID']
            if name == MasterSite['SUPPMWTag']:
                bFound = True
        if not bFound:
            nNumAdded +=1
            NewSite = dict(
                Name = MasterSite['Name'],
                LMPTag = MasterSite['LMPTag'],
                ResourceID = MasterSite['SUPPMWTag'],
                OIRows = [],
                LMPRows = [],
                DispatchRows = []
                )
            NewSiteList.append(NewSite)
    ## move this 
    if nNumAdded > 0:
        for item in NewSiteList:
            SiteRecords['Sites'].append(item)
    stop = 1


    WriteToDBWithLock(SiteRecords,lockName)

    return nNumAdded 


########### Operation Instruction #####################################
def AddNewOIToResource(CurrentResourceID,inOIFlag =None,inStartTime =None,inEndTime=None,
            inOIReason = None,inOIReasonCode = None):

    
    SiteRecords, lockName = GetCurrentSiteRecordsWithLock()

    for site in SiteRecords['Sites']:  ## this is how we walk through it 
        name = site['ResourceID'] # We are working with Resource tag 

        #Get the date sorted out 
        #if the time is passed remove the instrution 

        dCurrenttime = datetime.datetime.now()

        if name == CurrentResourceID:
            bfound = False
            for row in site['OIRows']:
                if type(row) == object :
                    if row['StartTime'] == inStartTime:
                        row['OIFlag'] = inOIFlag
                        row['EndTime'] = inEndTime
                        row['OIReason'] = inOIReason
                        row['OIReasonCode'] = inOIReasonCode
                        bfound = True


            if not bfound:
                OpRow = dict(
                OIFlag = inOIFlag,
                StartTime = inStartTime,
                EndTime = inEndTime,
                OIReason = inOIReason,
                OIReasonCode = inOIReasonCode)
                site['DispatchRows'].append(OpRow)



    WriteToDBWithLock(SiteRecords,lockName)
         
    return 1

########### LMP #############################
def AddNewLMPtoResource(CurrentResourceID,inLMP=None,inStartTime =None, inEndTime=None):



    SiteRecords, lockName = GetCurrentSiteRecordsWithLock()

    for site in SiteRecords['Sites']:  ## this is how we walk through it 
        name = site['LMPTag'] # We are working with LMP tag 

        #Get the date sorted out 
        #if the time is passed remove the instrution 

        dCurrenttime = datetime.datetime.now()

        if name == CurrentResourceID:
            bfound = False
            for row in site['LMPRows']:
                if type(row) == object :
                    if row['StartTime'] == inStartTime:
                        row['LMP'] = inLMP
                        row['EndTime'] = inEndTime
                        bfound = True


            if not bfound:
                OpRow = dict(
                LMP = inLMP,
                StartTime = inStartTime,
                EndTime = inEndTime )
                site['LMPRows'].append(OpRow)



    WriteToDBWithLock(SiteRecords,lockName)
         
    return 1


######################## Dispatchs : DOT and SUPPMW 
def AddNewDOTtoResource(CurrentResourceID,inDOT=None, inSUPPMW = None, inFolFlag = None,
              inStartTime =None, inEndTime=None):


    
    SiteRecords, lockName = GetCurrentSiteRecordsWithLock()

    for site in SiteRecords['Sites']:  ## this is how we walk through it 
        name = site['ResourceID'] # We are working with Resource tag 

        #Get the date sorted out 
        #if the time is passed remove the instrution 

        dCurrenttime = datetime.datetime.now()

        if name == CurrentResourceID:
            bfound = False
            for row in site['DispatchRows']:
                if type(row) == object :
                    if row['StartTime'] == inStartTime:
                        row['DOT'] = inDOT
                        row['FollowFlag'] = inFolFlag
                        row['SuppMW'] = inSUPPMW
                        row['EndTime'] = inEndTime
                        bfound = True


            if not bfound:
                OpRow = dict(
                DOT = inDOT,
                FollowFlag = inFolFlag,
                SuppMW = inSUPPMW,
                StartTime = inStartTime,
                EndTime = inEndTime )
                site['DispatchRows'].append(OpRow)



    WriteToDBWithLock(SiteRecords,lockName)
         
    return 1

def WriteToDB(Records):


    # Filename is SiteDataBase.json
    sRootPath = getPath()
    sFilePath = sRootPath + 'SiteDataBase.json'
    notopen = True
    sLockFileName = sRootPath + 'SiteDataBase.LOCK'
    while notopen :
        try:
            FileisReady = os.path.isfile(sFilePath) 
            if FileisReady:
                LMPFileLog.WriteLogOut('renaming f ' + sFilePath + ' to ' + sLockFileName)
                os.rename(sFilePath,sLockFileName)
                LMPFileLog.WriteLogOut('opening f ' + sLockFileName)
                outfile = open(sLockFileName, 'w')
                LMPFileLog.WriteLogOut('writing to f ' + sFilePath + ' to ' + sLockFileName)
                json.dump(Records, outfile )
                outfile.close()
                os.rename(sLockFileName,sFilePath)
                notopen=False
        except Exception as e:
            LMPFileLog.WriteLogOut('\n WRITE DB JSON LOCKED'+ e)
            time.sleep(1)
    return 1


def ReadCurrentSite(sRootPath,sSiteName):

    for site in list['Sites']:
        for item in site:
            if site[item] == val:
                return site['Name']

    return ''


def GetLMPRecordsForNextFile(CurrentTime,CurrentResourceID,SecondsFromStart):
    sOutRecord = ''
    #sLoc = ''
    #sTotalValue = ''

    SiteRecords = GetCurrentSiteRecords()
    currenttime = CurrentTime
    for item in SiteRecords['Sites'] :

        if item["LMPTag"] == CurrentResourceID:
            for lmprow in item['LMPRows']:
                if type(lmprow) == dict:
                    startdate = lmprow['StartTime']
                    sdtdate = datetime.datetime.strptime(startdate,"%Y-%m-%dT%H:%M:%SZ")
                    dttime = sdtdate - datetime.timedelta(seconds=SecondsFromStart)
                    if currenttime >= dttime and currenttime <= sdtdate:
                        sLoc = item['Name']
                        sOpDate = sdtdate.strftime('%d/%m/%Y %H:%M:%S')
                        sTotalValue = lmprow['LMP']
                        sCol4 = '1'
                        sOutRecord = sLoc +' 5MIN LMP;' +sOpDate +';' + sTotalValue + ';'+ sCol4

    #        sTotalValue = sLMP
    #    # 09/05/2019 07:40:00
    #    sOpDate = dtValDate.strftime('%d/%m/%Y %H:%M:%S')
    #    OutFile = sLoc +' 5MIN LMP;' +sOpDate +';' + sTotalValue + ';'+ sCol4
    
    return sOutRecord

def GetDOTRecordsForNextFile(CurrentTime,CurrentResourceID,SecondsFromStart):
    sOutRecord = ''
    #sLoc = ''
    #sTotalValue = ''
    SiteRecords = GetCurrentSiteRecords()
    currenttime = CurrentTime
    for item in SiteRecords['Sites'] :

        if item["ResourceID"] == CurrentResourceID:
            for lmprow in item['DispatchRows']:
                if type(lmprow) == dict:
                    startdate = lmprow['StartTime']
                    sdtdate = datetime.datetime.strptime(startdate,"%Y-%m-%dT%H:%M:%SZ")
                    dttime = sdtdate - datetime.timedelta(seconds=SecondsFromStart)
                    if currenttime >= dttime and currenttime <= sdtdate:
                        siteName = item['Name']
                        sOpDate = sdtdate.strftime('%d/%m/%Y %H:%M:%S')
                        dot= lmprow['DOT']
                        if dot:
                            rdot = float(dot)
                            dot = "{:20.10f}".format(rdot).strip()
                            strout = '{} DOT MW;{};{};1'.format(siteName,sOpDate,dot) 
                            sOutRecord = strout
                      
    return sOutRecord

def GetSuppMWRecordsForNextFile(CurrentTime,CurrentResourceID,SecondsFromStart):
    sOutRecord = ''
    #sLoc = ''
    #sTotalValue = ''
    SiteRecords = GetCurrentSiteRecords()
    currenttime = CurrentTime
    for item in SiteRecords['Sites'] :

        if item["ResourceID"] == CurrentResourceID:
            for lmprow in item['DispatchRows']:
                if type(lmprow) == dict:
                    startdate = lmprow['StartTime']
                    sdtdate = datetime.datetime.strptime(startdate,"%Y-%m-%dT%H:%M:%SZ")
                    dttime = sdtdate - datetime.timedelta(seconds=SecondsFromStart)
                    if currenttime >= dttime and currenttime <= sdtdate:
                        siteName = item['Name']
                        sOpDate = sdtdate.strftime('%d/%m/%Y %H:%M:%S')
                        suppmw= lmprow['SuppMW']
                        if suppmw:
                            rsuppmw = float(suppmw)
                            suppmw = "{:20.10f}".format(rsuppmw).strip()
                            strout = '{} SUPP MW;{};{};1'.format(siteName,sOpDate,suppmw) 
                            sOutRecord = strout
                      
    return sOutRecord

def GetOIReasonRecordsForNextFile(CurrentTime,CurrentResourceID,SecondsFromStart):
    sOutRecord = ''
    #sLoc = ''
    #sTotalValue = ''
    SiteRecords = GetCurrentSiteRecords()
    currenttime = CurrentTime
    for item in SiteRecords['Sites'] :

        if item["ResourceID"] == CurrentResourceID:
            for lmprow in item['OIRows']:
                if type(lmprow) == dict:
                    startdate = lmprow['StartTime']
                    sdtdate = datetime.datetime.strptime(startdate,"%Y-%m-%dT%H:%M:%SZ")
                    dttime = sdtdate - datetime.timedelta(seconds=SecondsFromStart)
                    if currenttime >= dttime and currenttime <= sdtdate:
                        siteName = item['Name']
                        sOpDate = sdtdate.strftime('%d/%m/%Y %H:%M:%S')
                        reason= lmprow['OIReasonCode']
                        if not reason:
                            reason = 0

                        strout = '{} OPR INS REASON;{};{};1'.format(siteName,sOpDate,reason) 
                        sOutRecord = strout
    return sOutRecord

def GetOIFlagRecordsForNextFile(CurrentTime,CurrentResourceID,SecondsFromStart):
    sOutRecord = ''
    #sLoc = ''
    #sTotalValue = ''
    SiteRecords = GetCurrentSiteRecords()
    currenttime = CurrentTime
    for item in SiteRecords['Sites'] :

        if item["ResourceID"] == CurrentResourceID:
            for lmprow in item['OIRows']:
                if type(lmprow) == dict:
                    startdate = lmprow['StartTime']
                    sdtdate = datetime.datetime.strptime(startdate,"%Y-%m-%dT%H:%M:%SZ")
                    dttime = sdtdate - datetime.timedelta(seconds=SecondsFromStart)
                    if currenttime >= dttime and currenttime <= sdtdate:
                        siteName = item['Name']
                        sOpDate = sdtdate.strftime('%d/%m/%Y %H:%M:%S')
                        flag= lmprow['OIFlag']
                        if flag:
                            flag = 1
                        else:
                            flag = 0
                        strout = '{} OPR INS FLAG;{};{};1'.format(siteName,sOpDate,flag) 
                        sOutRecord = strout
    return sOutRecord

def GetFollowDotFlagRecordsForNextFile(CurrentTime,CurrentResourceID,SecondsFromStart):
    sOutRecord = ''
    #sLoc = ''
    #sTotalValue = ''
    SiteRecords = GetCurrentSiteRecords()
    currenttime = CurrentTime
    for item in SiteRecords['Sites'] :

        if item["ResourceID"] == CurrentResourceID:
            for lmprow in item['DispatchRows']:
                if type(lmprow) == dict:
                    startdate = lmprow['StartTime']
                    sdtdate = datetime.datetime.strptime(startdate,"%Y-%m-%dT%H:%M:%SZ")
                    dttime = sdtdate - datetime.timedelta(seconds=SecondsFromStart)
                    if currenttime >= dttime and currenttime <= sdtdate:
                        siteName = item['Name']
                        sOpDate = sdtdate.strftime('%d/%m/%Y %H:%M:%S')
                        followflag = lmprow['FollowFlag']
                        if followflag:
                            followflag = 1
                        else:
                            followflag = 0
                        strout = '{} FOLLOW DOT FLAG;{};{};1'.format(siteName,sOpDate,followflag) 
                        sOutRecord = strout
                      
    return sOutRecord
