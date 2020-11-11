import datetime



class ADSUtil():
    def __init__(self):


        self.MaxDate = None
        self.UTCMaxDate = None
        self.CurrentOffSet = -7

    def GetMaxDate(self,IntervalInMin):
        if self.MaxDate:
            return self.MaxDate
        azoffset = -7
        rdate = datetime.datetime.utcnow()
        iMin = int(rdate.strftime('%M'))
        iHour = int(rdate.strftime('%H'))
        iCurrTens = iMin//10   
        iCurrOnes = iMin % 10
        if iCurrOnes - 5 >= 0:
            iCurrTens += 1
            iCurrOnes = 0
        else:
            iCurrOnes = 5
        if iCurrTens > 5: 
            iCurrTens = 0
            iHour += 1
        if iHour > 23: 
            #ok now we are in a new day 
            iHour = 0;
            rdate = rdate +  datetime.timedelta(seconds = 600) # add 10 mins
         

        sDate = rdate.strftime('%Y-%m-%dT' +str(iHour)+':'+str(iCurrTens)+str(iCurrOnes)+':00')
        ndate = datetime.datetime.strptime(sDate,'%Y-%m-%dT%H:%M:%S')
        self.UTCMaxDate = ndate
        self.MaxDate  = ndate + datetime.timedelta(seconds=(azoffset*60*60)) 
        return self.MaxDate




