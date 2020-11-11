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
import threading

# CAISO moduals 
import LMPFileLog 

import CAISOLMP

import LMPSiteDataModel

##import queue

# this sets it to run the first time , the config file will turn it off if set to 0 
KeepRunning = True
#### DBFileState = queue.Queue()

#get site configuration from json file 
def GetSiteConfig():
    with open('SiteMaster.json') as json_file:  
        sites= json.load(json_file)
    return sites



##############################################################################

import threading, msvcrt
import sys

def readInput(caption, default, timeout = 5):
    class KeyboardThread(threading.Thread):
        def run(self):
            self.timedout = False
            self.input = ''
            while True:
                if msvcrt.kbhit():
                    chr = msvcrt.getche()
                    if ord(chr) == 13:
                        break
                    elif ord(chr) >= 32:
                        self.input += str(chr)
                if len(self.input) == 0 and self.timedout:
                    break    


    sys.stdout.write('%s(%s):'%(caption, default));
    result = default
    it = KeyboardThread()
    it.start()
    it.join(timeout)
    it.timedout = True
    if len(it.input) > 0:
        # wait for rest of input
        it.join()
        result = it.input
    print( '')  # needed to move to next line
    return result




#############################################################################




##########################################################################################################################################
######## Start Main
############################################################################################################################################

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',level=logging.DEBUG)
logging.warning('This will get logged to a file')



try: 
    ### WE LET THE THREADS WORK THE LOOP 
   #### get all the setups 

    #global vars 
    sRoot = ''
    sOutboundDir = ''
    sUnProcessedDir = ''
    fRuntime = 0.0
    # set the time stamp
    t = datetime.datetime.now()
    

    dttime = datetime.datetime.now() - datetime.timedelta(weeks=4)

    dtslastruntime = dttime.timestamp()
    #set the data the first time we run (defaults ) 
    configfileexist = os.path.isfile('lmpjobconfig.json') 
    configdata = {} 

    if not configfileexist: 

        configdata={
            'root': 'C:\\TEMPTEST\\FTPSiteTest\\',
            'inbounddir': 'Inbound',
            'outbounddir': 'OutBound',
            'unprocesseddir': 'Unprocessed',
            'runtime': str(tslastruntime)
            }


        with open('lmpjobconfig.json', 'w') as outfile:  
            json.dump(configdata, outfile)
    else:
        with open('lmpjobconfig.json') as json_file:  
            configdata = json.load(json_file)

            LMPFileLog.WriteLogOut('\ni opened the file')
            


    c= configdata
    sRoot = c['root']
    sInboundDir = c['inbounddir']
    sOutboundDir = c['outbounddir']
    sUnProcessedDir = c['unprocesseddir']
    fRuntime = float(c['runtime'])
    nKeepRunning = int(c['KeepRunning'])
    nLMPPrcTimeMax = int(c['LMPProcessingTimeMax'])
    sBaseURL = c['iporurl']
    if not nKeepRunning:
        KeepRunning = False

    # tsMaxDate = GetMaxDate(5) # pass the interval in min .. CAISO use config later 

    LMPFileLog.WriteLogOut('PROCESS START' + t.strftime("%b %d %Y %H:%M:%S.%f"))  

    # set the root  " ### RWJEDIT
    path = sRoot + sInboundDir +'\\'
    #RWJTEST
    MySiteList = GetSiteConfig()

    while KeepRunning:

        # and some examples of usage

        time.sleep(5)

        try: 
            for site in MySiteList['Sites']:
            
                sCurrSite= site['LMPTag']
                sCurrLoc = site['Name']
                LMPFileLog.WriteLogOut('Init LMP ::: Next LMP To:'+ sCurrLoc )
                sout = CAISOLMP.GetLMPFromCaisowithTagArray(sBaseURL,"temp_csv//",sCurrSite,sCurrLoc)
        except Exception as e:
            LMPFileLog.WriteLogOut('Exception ' + str(e))

        LMPFileLog.WriteLogOut('FILE CREATE END {}'.format(datetime.datetime.utcnow().strftime('%d/%m/%Y %H:%M:%S'))    )



        print('\n###################################################################################################################################################################################################################################')
        print('\n###################################################################################################################################################################################################################################')
        print('\n')
        ans = readInput('Press N to Stop Execution: Keep Running? ', 'Y') 
        if not  str(ans).upper() =='Y' :
            KeepRunning = 0
            print('###################################################################################################################################################################################################################################')
            print('# PROGRAM ENDING #########################################################################################################################################################################################################')


    #LMPProcess.ENDPROC = 1 

except Exception as e:
    LMPFileLog.WriteLogOut(e)

os._exit(0)




