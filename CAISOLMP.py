import os
import time
import json
import datetime
import string
import shutil
import logging
import LMPFileLog
import SiteDBModel


import urllib
from urllib import request
from  zipfile import ZipFile
import csv
#Include "RowDataBase sections 
import LMPSiteDataModel



#now get the dat out of the file 
def GetLatestValue(path,infile,node):
    #unzip thefile 
    maxdate = datetime.datetime.utcnow() - datetime.timedelta(seconds=600)
    currLMP = 'NOFILE'
    with ZipFile(infile, 'r') as zipObj:
        # Get a list of all archived file names from the zip
        listOfFileNames = zipObj.namelist()
        # Iterate over the file names
        CurrentCSVFile = ''
        for fileName in listOfFileNames:
            # Check filename endswith csv
            if fileName.endswith('.csv') and 'LMP_RTM' in fileName:
                # Extract a single file from zip
                CurrentCSVFile = fileName
                zipObj.extract(fileName, 'temp_csv')
    if len(CurrentCSVFile) == 0:
        #NoFile exit out 
        LMPFileLog.WriteLogOut('no file for node : '+ node)
        return maxdate,currLMP
    # OK Ihave a file
    f = open('temp_csv\\'+ CurrentCSVFile)
    fcontent = csv.reader(f)

    for row in fcontent:
        
        if 'LMP_PRC' in row and node in row:
            sdate = row[0]
            edate = row[1]
            currLMP = row[13]
            sdtdate = datetime.datetime.strptime(sdate,"%Y-%m-%dT%H:%M:00-00:00")
            edtdate = datetime.datetime.strptime(edate,"%Y-%m-%dT%H:%M:00-00:00")
            sdate = sdtdate.strftime('%Y-%m-%dT%H:%M:%SZ')
            edate = edtdate.strftime('%Y-%m-%dT%H:%M:%SZ')

            ## 2020-09-24T19:37:30Z
            # string to date 
            try: 
                nCheck = SiteDBModel.AddNewDBLMPtoResource(node,currLMP,sdate, edate)
            except Exception as e:
                LMPFileLog.WriteLogOut ( 'ADD LMP ERRRER ' + str(e))
            # 2019-05-09T16:10:00-00:00
            ##cdate = datetime.datetime.strptime(sdate,"%Y-%m-%dT%H:%M:00-00:00")
            #if cdate > maxdate:
                ##maxdate = cdate
                #currLMP = row[13]
            #sCurrentTime = cdate.strftime("%Y%m%d%H%M")
        #val = row[9]
    
    return maxdate, currLMP
        
    #now you have the data 
    # get the lastest price

#Get the LMP File Directly from CAISO
def GetLMPFromCaiso(outpath,sitelist):
    # get the dates we care about 
    #20190509T16:00-0000
    sCurrentTime = datetime.datetime.now().strftime("%Y%m%d%H%M")
    dttime = datetime.datetime.utcnow() - datetime.timedelta(seconds=600)
    sStartTime=dttime.strftime("%Y%m%d") + 'T' + dttime.strftime("%H:%M") + "-0000"
    
    dttime = datetime.datetime.utcnow() + datetime.timedelta(seconds=300)
    sEndTime=dttime.strftime("%Y%m%dT%H:%M-0000")

    #setup the outfile array
    OutFile = []
    #FILE FORMAT 
    #LOCATION;START_TIMESTAMP;CAISO_DOT_MW;COLUMN_4
    #Agua Caliente  5MIN LMP;09/05/2019 07:40:00;68.2500000000;1
    # ADD TOP ROW 
    OutFile.append('LOCATION;OPERATING_DATE;TOTAL_VALUE;COLUMN_4')
    sLoc = ''
    sOpDate =''
    sTotalValue =''
    sCol4 = '1'
    #sURL = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_INTVL_LMP&version=3&startdatetime={}&enddatetime={}&market_run_id=RTM&grp_type=ALL".format(sStartTime,sEndTime)
    #soutf = outpath + 'LMP' + sCurrentTime + '.new.zip'

    for site in sitelist['Sites']:
        
        nCount = 0;
        sCurrSite = site['LMPTag']
        sURL = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_INTVL_LMP&version=3&startdatetime={}&enddatetime={}&market_run_id=RTM&node={}".format(sStartTime,sEndTime,sCurrSite)
        soutf = outpath + 'LMP' + sCurrentTime + '.new.zip'
        while nCount < 8: # over 8 sec delay we have given up  
            try:
             
                # urllib.request.urlretrieve(sURL,soutf)
                with urllib.request.urlopen(sURL) as response:
                    LMPFileLog.WriteLogOut('getting Current Site {}'.format(sCurrSite)  )
                    with open(soutf, 'wb') as tmp_file:  
                        shutil.copyfileobj(response, tmp_file)
                nCount = 100 # we got a file . .and we are moving on 
                time.sleep(5)
            except Exception as e:
                LMPFileLog.WriteLogOut(e)
                # we did not get a file .. weare going to wait a bit
                nCount += 1
                #ok get the site list as apparently this doesn work anymore
                time.sleep(5)
                LMPFileLog.WriteLogOut('file try {} : Current Site {}'.format(str(nCount),sCurrSite))

        sCurrSite = site['LMPTag']
        sLoc = site['Name']
        dtValDate, sLMP = GetLatestValue(outpath,soutf,sCurrSite)
        #if there is no file at this time continue
        if  'NOFILE' not in sLMP:

            sTotalValue = sLMP
            # 09/05/2019 07:40:00
            sOpDate = dtValDate.strftime('%d/%m/%Y %H:%M:%S')
            OutFile.append(sLoc +' 5MIN LMP;' +sOpDate +';' + sTotalValue + ';'+ sCol4)
        else:
            LMPFileLog.WriteLogOut('do data for '+sCurrSite)


    return OutFile
   


def GetLMPFromCaisowithTagArray(baseurls,outpath,tag,location):
    # get the dates we care about 
    #20190509T16:00-0000
    sCurrentTime = datetime.datetime.now().strftime("%Y%m%d%H%M")
    dttime = datetime.datetime.utcnow() - datetime.timedelta(seconds=300) # 5 mins before
    sStartTime=dttime.strftime("%Y%m%d") + 'T' + dttime.strftime("%H:%M") + "-0000"
    
    dttime = datetime.datetime.utcnow() + datetime.timedelta(seconds=3200) #24 min after
    sEndTime=dttime.strftime("%Y%m%dT%H:%M-0000")

    #setup the outfile array
    #OutFile = []
    sLoc = ''
    sOpDate =''
    sTotalValue =''
    sCol4 = '1'
    #sURL = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_INTVL_LMP&version=3&startdatetime={}&enddatetime={}&market_run_id=RTM&grp_type=ALL".format(sStartTime,sEndTime)
    #soutf = outpath + 'LMP' + sCurrentTime + '.new.zip'


        
    nCount = 0;
    sCurrSite = tag
  # 12.231.58.66
  #  sURL = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_INTVL_LMP&version=3&startdatetime={}&enddatetime={}&market_run_id=RTM&node={}".format(sStartTime,sEndTime,sCurrSite)



  ## change this to read from file n
    sURL = "http://" + baseurls + "/oasisapi/SingleZip?resultformat=6&queryname=PRC_INTVL_LMP&version=3&startdatetime={}&enddatetime={}&market_run_id=RTM&node={}".format(sStartTime,sEndTime,sCurrSite)

    iplist = baseurls.split(',')
    soutf = outpath + 'LMP' + sCurrentTime + '.new.zip'
    #Setup the array of IP Address to work through if we need to 
    nListMax =len(iplist)
    nListCurr = 0
    while nCount < 8: # over 8 sec delay we have given up  
        try:
            #Get the right IP 
            if nListCurr >= nListMax:
                nListCurr = 0
            sURL = "http://" + iplist[nListCurr] + "/oasisapi/SingleZip?resultformat=6&queryname=PRC_INTVL_LMP&version=3&startdatetime={}&enddatetime={}&market_run_id=RTM&node={}".format(sStartTime,sEndTime,sCurrSite)

            # urllib.request.urlretrieve(sURL,soutf)
            with urllib.request.urlopen(sURL) as response:
                LMPFileLog.WriteLogOut('getting Current Site {} :: {} ip {}'.format(sCurrSite,location,iplist[nListCurr])  )
                with open(soutf, 'wb') as tmp_file:  
                    shutil.copyfileobj(response, tmp_file)
            nCount = 100 # we got a file . .and we are moving on 
            #time.sleep(6)
        except Exception as e:
            LMPFileLog.WriteLogOut(e)
            # we did not get a file .. weare going to wait a bit
            nListCurr += 1
            nCount += 1
            #ok get the site list as apparently this doesn work anymore
            time.sleep(5)
            LMPFileLog.WriteLogOut('file try {} : Current Site {}'.format(str(nCount),sCurrSite))

    sCurrSite = tag
    sLoc = location
    dtValDate, sLMP = GetLatestValue(outpath,soutf,sCurrSite)
    #if there is no file at this time continue
    OutFile = ''
    if  'NOFILE' not in sLMP:

        sTotalValue = sLMP
        # 09/05/2019 07:40:00
        sOpDate = dtValDate.strftime('%d/%m/%Y %H:%M:%S')
        OutFile = sLoc +' 5MIN LMP;' +sOpDate +';' + sTotalValue + ';'+ sCol4
    else:
        LMPFileLog.WriteLogOut('do data for '+sCurrSite)


    return OutFile