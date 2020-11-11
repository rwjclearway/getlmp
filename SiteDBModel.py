# first read the most recent vlue in the directory 
import os
import time
import json
import datetime
import string
import shutil
import logging

import collections
import LMPFileLog
import uuid
import sqlite3 




########################################## Helper Functions ##################################

def getPath():
    with open('lmpjobconfig.json') as json_file:  
        configdata = json.load(json_file)
    sPath = configdata['root']
    return sPath

########################################## Data Base Setups ##################################
def ConnectToDB():
    try:
        sroot = getPath()
        sPathAndRoot = sroot + '\\SiteDispatch.db'
        conn = sqlite3.connect(sPathAndRoot ) 
        return conn
    except Exception as e:
        LMPFileLog.WriteLogOut("Database Failed ")
    return None



def table_exists(table_name): 
    dbconn = ConnectToDB()
    c = dbconn.cursor()
    c.execute('''SELECT count(name) FROM sqlite_master WHERE TYPE = 'table' AND name = '{}' '''.format(table_name)) 
    if c.fetchone()[0] == 1: 
        return True 
    dbconn.close()
    return False


def InitDB():
    ## Check the existance of 4 tables 
    # SiteMaster
    #   ID INTEGER ROWID Will be used 
    #   Name
    #   LMPTag
    #   ResourceID
    # IORecords
    #   SiteMasterID INTEGER
    #   StartTime TEXT
    #   OIFlag
    #   EndTime'TEXT 
    #   OIReason
    #   OIReasonCode
    # LMPRecords 
    #   SiteMasterID INTEGER
    #   StartTime TEXT
    #   LMP
    #   EndTime
    # DispatchRecords
    #   SiteMasterID INTEGER
    #   StartTime TEXT
    #   EndTime  
    #   DOT
    #   FollowFlag    
    #   SuppMW
    dbconn = ConnectToDB()
    c = dbconn.cursor()
    if not table_exists('SiteMaster'): 
        c.execute(''' 
            CREATE TABLE SiteMaster( 
                name TEXT, 
                LMPTag TEXT, 
                ResourceID TEXT
            ) 
        ''')



    dbconn.commit()
    if not table_exists('IORecords'): 
        c.execute(''' 
            CREATE TABLE IORecords( 
                SiteMasterID INTEGER, 
                StartTime TEXT,
                OIFlag TEXT,
                EndTime TEXT ,
                OIReason TEXT,
                OIReasonCode TEXT
            ) 
        ''')
    if not table_exists('LMPRecords'): 
        c.execute(''' 
            CREATE TABLE LMPRecords( 
                SiteMasterID INTEGER, 
                StartTime TEXT,
                EndTime TEXT,
                LMP TEXT
            ) 
        ''')
    if not table_exists('DispatchRecords'): 
       # {"DOT": "0.0", "FollowFlag": null, "SuppMW": "-226.96", "StartTime": "2020-09-28T17:30:00Z", "EndTime": "2020-09-28T17:30:00Z"}
        c.execute(''' 
            CREATE TABLE DispatchRecords( 
                SiteMasterID INTEGER, 
                StartTime TEXT,
                EndTime TEXT ,
                FollowFlag TEXT,
                DOT TEXT,
                SuppMW TEXT
            ) 
        ''')
    dbconn.close()
    return 1

def GetDBSiteByName(name): 
    dbconn = ConnectToDB()
    c = dbconn.cursor()
    c.execute('''SELECT ROWID,a.a FROM SiteMaster a WHERE a.Name = {}'''.format(name)) 
    data = [] 
    for row in c.fetchall():  
        data.append(row) 
    
    dbconn.close()
    return data

def GetDBSiteMasterRecords():
    sitelist =[]
    dbconn = ConnectToDB()
    c = dbconn.cursor()
    c.execute('''SELECT ROWID, a.* FROM SiteMaster a''') 
    sitelist = [] 
    for row in c.fetchall(): 
        sitelist.append(row) 
        ## sitelist[row][0] = rowid
        ## sitelist[row][1] = Name
        ## sitelist[row][2] = LMPTag
        ## sitelist[row][3] = ResourceID
    dbconn.close()
    return sitelist 

def GetDBSiteMasterRecord(rowid = None,Name=None, LMPTag=None,ResourceID=None):
    sitelist =[]
    dbconn = ConnectToDB()
    c = dbconn.cursor()

    # Build Where Clause

    wherecls = ''
    if not rowid  == None:
        wherecls += " a.ROWID = {} ".format(str(rowid))
    if not Name  == None:
        if len(wherecls) > 0 : 
            wherecls += ' AND '
        wherecls += " a.Name = '{}' ".format(str(Name))
    if not LMPTag  == None:
        if len(wherecls) > 0 : 
            wherecls += ' AND '
        wherecls += " a.LMPTag = '{}' ".format(str(LMPTag))
    if not ResourceID  == None:
        if len(wherecls) > 0 : 
            wherecls += ' AND '
        wherecls += " a.ResourceID = '{}' ".format(str(ResourceID))
    

    sql = ''
    sql +=' '' '   # Start quote
    sql += 'SELECT ROWID, a.* FROM SiteMaster a '
    if len(wherecls) > 0 : 
        sql += 'WHERE ' + wherecls
    
    #and Where Clause 
    sql +=' '' '   # end quote
    c.execute(sql) 
    sitelist = [] 
    for row in c.fetchall(): 
        sitelist.append(row) 
        ## sitelist[row][0] = rowid
        ## sitelist[row][1] = Name
        ## sitelist[row][2] = LMPTag
        ## sitelist[row][3] = ResourceID

    dbconn.close()
    return sitelist 

def InitDBSiteDataModel(CurrentMasterSiteList):
    nNumAdded = 0

    NewSiteList = [];
    dbconn = ConnectToDB()
    c = dbconn.cursor()
   # c.execute('''DELETE from SiteMaster''')
    
    SiteRecords = GetDBSiteMasterRecords()
    dbconn.commit()
    for MasterSite in CurrentMasterSiteList['Sites']:
        bFound = False
        SiteRecords = GetDBSiteMasterRecord(None,None,None,MasterSite['SUPPMWTag'])
        for site in SiteRecords:  ## this is how we walk through it 
            x=site[0]
            name = site[3] #ResourceID
            if name == MasterSite['SUPPMWTag']:
                bFound = True
        if not bFound:
            Name = str(MasterSite['Name'])
            LMPTag = str(MasterSite['LMPTag'])
            ResourceID = str(MasterSite['SUPPMWTag'])
            try:
                c.execute(''' INSERT INTO SiteMaster ( name, LMPTag, ResourceID) VALUES(?, ?, ?) ''', (str(Name),str(LMPTag),str(ResourceID)))
                dbconn.commit()
                nNumAdded +=1
            except Exception as e:
                LMPFileLog.WritetoFile('Error in db insert' + str(e))
        
    ## move this 

    dbconn.close()
    return nNumAdded 




########### LMP #############################

def AddNewDBLMPtoResource(CurrentResourceID,inLMP=None,inStartTime =None, inEndTime=None):


    # Get the Current Site ID 
    SiteRecord = GetDBSiteMasterRecord(None,None,CurrentResourceID,None)
    if len(SiteRecord) <= 0:
        return 0 # not one of our resources
    SiteID = SiteRecord[0][0]

    insertsql = ''
    insertsql += " INSERT INTO LMPRecords ( SiteMasterID, StartTime, EndTime, LMP )  VALUES ( ?,?,?,?) "

    dbconn = ConnectToDB()
    c = dbconn.cursor()

    c.execute(insertsql,(int(SiteID), str(inStartTime),str(inEndTime),str(inLMP) ) )
    dbconn.commit()
    
    dbconn.close()  
    return 1


########### Operation Instruction #####################################
def AddNewDBOIToResource(CurrentResourceID,inOIFlag =None,inStartTime =None,inEndTime=None,
            inOIReason = None,inOIReasonCode = None):


    # Get the Current Site ID 
    SiteRecord = GetDBSiteMasterRecord(None,None,None,CurrentResourceID)
    if len(SiteRecord) <= 0:
        return 0 # not one of our resources
    SiteID = SiteRecord[0][0]
    
        ##SiteMasterID INTEGER, 
        ##        StartTime TEXT,
        ##        OIFlag TEXT,
        ##        EndTime TEXT ,
        ##        OIReason TEXT,
        ##        OIReasonCode TEXT

    insertsql = ''
    insertsql += " INSERT INTO IORecords ( SiteMasterID, StartTime, EndTime, OIFlag , OIReason, OIReasonCode)  VALUES ( ?,?,?,?,?,?) "

    dbconn = ConnectToDB()
    c = dbconn.cursor()
    if inFolFlag == None:
        inFolFlag  ='0'
    c.execute(insertsql,(int(SiteID), str(inStartTime),str(inEndTime),str(inOIFlag), str(inOIReason),str(inOIReasonCode) ) )
    dbconn.commit()
    dbconn.close()
         
    return 1


######################## Dispatchs : DOT and SUPPMW 
def AddNewDBDOTtoResource(CurrentResourceID,inDOT=None, inSUPPMW = None, inFolFlag = None,
              inStartTime =None, inEndTime=None):


    # Get the Current Site ID 
    SiteRecord = GetDBSiteMasterRecord(None,None,None,CurrentResourceID)
    if len(SiteRecord) <= 0:
        return 0 # not one of our resources
    SiteID = SiteRecord[0][0]

    insertsql = ''
    insertsql += " INSERT INTO DispatchRecords ( SiteMasterID, StartTime, EndTime, DOT , FollowFlag, SuppMW)  VALUES ( ?,?,?,?,?,?) "

    dbconn = ConnectToDB()
    c = dbconn.cursor()
    if inFolFlag == None:
        inFolFlag  ='0'
    c.execute(insertsql,(int(SiteID), str(inStartTime),str(inEndTime),str(inDOT), str(inFolFlag),str(inDOT) ) )
    dbconn.commit()
    dbconn.close()
         
    return 1

#### Get Records ############################ 

def GetDBDOTRecordsForNextFile(CurrentTime,CurrentResourceID,SecondsFromStart):
    sOutRecord = ''
    #sLoc = ''
    #sTotalValue = ''
    currenttime = CurrentTime
    SiteRecords = GetDBSiteMasterRecord(None,None,None,CurrentResourceID)
    SiteID = SiteRecords[0][0]
    dbconn = ConnectToDB()
    c = dbconn.cursor()

    # Build Where Clause

    wherecls = ''
    if not CurrentResourceID == None:
        wherecls += " a.SiteMasterID = {} ".format(str(SiteID))

    

    sql = ''
    sql += ' SELECT ROWID, a.* FROM DispatchRecords a'
    if len(wherecls)> 0 :
        sql += ' WHERE ' + wherecls
    
    sql += ' ORDER BY ROWID ASC'
    c.execute(sql) 
    for row in c.fetchall(): 

        ##SiteMasterID 1
        ##StartTime 2
        ##EndTime 3
        ##FollowFlag 4
        ##DOT 5
        ##SuppMW 6 


    

        startdate = row[2]
        sdtdate = datetime.datetime.strptime(startdate,"%Y-%m-%dT%H:%M:%SZ")

        #set currenttime 2020-10-06T20:04:00Z
        #RWJTEST REMOVE
        currenttime = datetime.datetime.strptime('2020-10-06T20:04:00Z',"%Y-%m-%dT%H:%M:%SZ")

        dttime = sdtdate - datetime.timedelta(seconds=SecondsFromStart)
        if currenttime >= dttime and currenttime <= sdtdate:
            siteName = SiteRecords[0][1]
            sOpDate = sdtdate.strftime('%d/%m/%Y %H:%M:%S')
            dot= row[5]
            if dot:
                rdot = float(dot)
                dot = "{:20.10f}".format(rdot).strip()
                strout = '{} DOT MW;{};{};1'.format(siteName,sOpDate,dot) 
                sOutRecord = strout
    
    dbconn.close()
    return sOutRecord

def GetDBSuppMWRecordsForNextFile(CurrentTime,CurrentResourceID,SecondsFromStart):
    sOutRecord = ''
    #sLoc = ''
    #sTotalValue = ''
    currenttime = CurrentTime
    SiteRecords = GetDBSiteMasterRecord(None,None,None,CurrentResourceID)
    SiteID = SiteRecords[0][0]
    dbconn = ConnectToDB()
    c = dbconn.cursor()

    # Build Where Clause

    wherecls = ''
    if not CurrentResourceID == None:
        wherecls += " a.SiteMasterID = {} ".format(str(SiteID))

    

    sql = ''
    sql += ' SELECT ROWID, a.* FROM DispatchRecords a'
    if len(wherecls)> 0 :
        sql += ' WHERE ' + wherecls
    
    sql += ' ORDER BY ROWID ASC'
    c.execute(sql) 
    for row in c.fetchall(): 

        ##SiteMasterID 1
        ##StartTime 2
        ##EndTime 3
        ##FollowFlag 4
        ##DOT 5
        ##SuppMW 6 

        startdate = row[2]
        sdtdate = datetime.datetime.strptime(startdate,"%Y-%m-%dT%H:%M:%SZ")

        #set currenttime 2020-10-06T20:04:00Z
        #RWJTEST REMOVE
        currenttime = datetime.datetime.strptime('2020-10-06T20:04:00Z',"%Y-%m-%dT%H:%M:%SZ")

        dttime = sdtdate - datetime.timedelta(seconds=SecondsFromStart)
        if currenttime >= dttime and currenttime <= sdtdate:
            siteName = SiteRecords[0][1]
            sOpDate = sdtdate.strftime('%d/%m/%Y %H:%M:%S')
            suppmw= row[6]
            if suppmw:
                rsuppmw = float(suppmw)
                suppmw = "{:20.10f}".format(rsuppmw).strip()
                strout = '{} SUPP MW;{};{};1'.format(siteName,sOpDate,suppmw) 
                sOutRecord = strout
    dbconn.close()                  
    return sOutRecord

def GetDBFollowDotFlagRecordsForNextFile(CurrentTime,CurrentResourceID,SecondsFromStart):
    sOutRecord = ''
    #sLoc = ''
    #sTotalValue = ''
    currenttime = CurrentTime
    SiteRecords = GetDBSiteMasterRecord(None,None,None,CurrentResourceID)
    SiteID = SiteRecords[0][0]
    dbconn = ConnectToDB()
    c = dbconn.cursor()

    # Build Where Clause

    wherecls = ''
    if not CurrentResourceID == None:
        wherecls += " a.SiteMasterID = {} ".format(str(SiteID))

    

    sql = ''
    sql += ' SELECT ROWID, a.* FROM DispatchRecords a'
    if len(wherecls)> 0 :
        sql += ' WHERE ' + wherecls
    
    sql += ' ORDER BY ROWID ASC'
    c.execute(sql) 
    for row in c.fetchall(): 

        ##SiteMasterID 1
        ##StartTime 2
        ##EndTime 3
        ##FollowFlag 4
        ##DOT 5
        ##SuppMW 6 

        startdate = row[2]
        sdtdate = datetime.datetime.strptime(startdate,"%Y-%m-%dT%H:%M:%SZ")

        #set currenttime 2020-10-06T20:04:00Z
        #RWJTEST REMOVE
        currenttime = datetime.datetime.strptime('2020-10-06T20:04:00Z',"%Y-%m-%dT%H:%M:%SZ")

        dttime = sdtdate - datetime.timedelta(seconds=SecondsFromStart)
        if currenttime >= dttime and currenttime <= sdtdate:
            siteName = SiteRecords[0][1]
            sOpDate = sdtdate.strftime('%d/%m/%Y %H:%M:%S')

            followflag = row[4]
            if followflag:
                followflag = 1
            else:
                followflag = 2
            strout = '{} FOLLOW DOT FLAG;{};{};1'.format(siteName,sOpDate,followflag) 
            sOutRecord = strout

    dbconn.close()                      
    return sOutRecord

def GetDBDispatchRecords(ResourceID=None):
    rowlist = []
    SiteRecords = GetDBSiteMasterRecord(None,None,None,ResourceID)
    SiteID = SiteRecords[0]
    dbconn = ConnectToDB()
    c = dbconn.cursor()

    # Build Where Clause

    wherecls = ''
    if not ResourceID == None:
        wherecls += " a.SiteMasterID = {} ".format(str(SiteID))

    

    sql = ''
    sql +=' '' '   # Start quote
    sql += 'SELECT ROWID, a.* FROM DispatchRecords a'
    if len(wherecls)> 0 :
        sql += 'WHERE ' + wherecls
    
    #and Where Clause 
    sql +=' '' '   # end quote
    c.execute(sql) 
    for row in c.fetchall(): 
        rowlist.append(row) 
        ## sitelist[row][0] = rowid
        ## sitelist[row][1] = Name
        ## sitelist[row][2] = LMPTag
        ## sitelist[row][3] = ResourceID
        #


    dbconn.close()
    return rowlist 







##################################### End Database ############################################

