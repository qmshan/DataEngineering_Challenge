from sys import argv
import heapq
import time
import datetime
from collections import deque
from sets import Set
import sys

"""
log class is used to store log information after string parsing
"""
class log(object):
    def __init__(self, IP, time, stamp, resource, status, size, line):
        self.IP = IP
        self.time = time
        self.stamp = stamp
        self.resource = resource
        self.status = status
        self.size = size
        self.line = line

    #For debug print out only
    def print_out(self):
        print "------------------------NEW LINE------------------------------------"
        print self.IP
        print self.time
        print self.stamp
        print self.resource
        print self.status
        print self.size
        print self.line
        return

"""
The function to transform time string into actual seconds
"""
def timeTrans(t):
    monthMap = {"Jan":"01", "Feb": "02",  "Mar": "03", "Apr":"04", "May":"05", "Jun":"06", "Jul":"07", "Aug":"08", "Sep":"09", "Oct":"10", "Nov": "11", "Dec": "12"}
    t = t[:3] + monthMap[t[3:6]] + t[6:20]
    return  int(time.mktime(time.strptime(t, '%d/%m/%Y:%H:%M:%S')))
   
"""
The function to transform actual seconds into time string
"""    
def timeReverse(stamp):    
    monthReverse = {"01":"Jan", "02":"Feb", "03":"Mar", "04":"Apr", "05":"May", "06":"Jun", "07":"Jul", "08":"Aug", "09":"Seq", "10":"Oct", "11":"Nov", "12":"Dec"}
    time_str = datetime.datetime.fromtimestamp(stamp).strftime('%d/%m/%Y:%H:%M:%S')
    return time_str[:3] + monthReverse[time_str[3:5]] + time_str[5:19] + " -0400"

"""
The function to parse a single line log into program specified structure
Many corner cases included, as parsing failure may occur if a log line is not irregular .
"""    
def parse(line):  
    if not checkValid(line):
        return None
    visit = line.split(" - - ")
    if len(visit) < 2:
        return None
    IP = visit[0]
    info = visit[1].split("\"")
    if len(info) < 3:
        return None

    tmp = info[0].strip()
    if (not len(tmp)):
        return None

    time = tmp.strip('[]')
    stamp =timeTrans(time)
    tmp = info[1].split(" ")
    if len(tmp) < 2:
        return None

    resource = tmp[1]
    tmp = info[2].strip()
    if (not len(tmp)):
        return None

    ss= tmp.split(" ")
    if len(ss) < 2:
        return None
    
    if not len(ss[0]) or not len(ss[1]):
        return None

    s, size= ss[0].strip(),ss[1].strip()
    if (size == "-"):
        size = 0
    else:
        size = int(size)
    if s == "401":
        status = False
    else:
        status = True
    item = log(IP, time, stamp, resource, status, size, line)
    return item

"""
The function to check if the current input log line is valid
"""
def checkValid(line):
    ndash = line.count(" - - ")
    nlb = line.count('[')
    nrb = line.count(']')
    nq = line.count('\"')
    return ndash == 1 and nlb == 1 and nrb == 1 and nq == 2

"""
Feature 1: 
List in descending order the top 10 most active hosts/IP addresses 
that have accessed the site.
"""
class TopIP(object):
    def __init__(self, fname):
        self.fname = fname
        self.dict = {} # hash is used to store the visit time of each ip
        return
    
    def getItem(self, item):
        ip = item.IP;
        if ip in self.dict:
            self.dict[ip] += 1
        else:
            self.dict[ip] = 1
        return    
        
    def output(self):
        heap = []
        heapq.heapify(heap) #priority Q is used to store in the ip with the most visit time.
        
        topIpList = []
        for ip, count in self.dict.items():
            if len(heap) < 10:
                heapq.heappush(heap, (count, ip))
            else:
                if count >= heap[0][0]: #if visit time the ip >= the smallest of the current top 10 ip
                    heapq.heappop(heap)
                    heapq.heappush(heap, (count, ip))  #replace the smallest with the current ip
        while heap: # write current heap result into an array in ascending order
            count, ip = heapq.heappop(heap)
            topIpList.append(ip + "," + str(count))
        f = open(self.fname, 'w')
        while topIpList: #write to file in descending order
            currline = topIpList.pop()
            f.write(currline + "\n")
        f.close()
        return

"""
Feature 2: 
Identify the top 10 resources on the site that consume the most bandwidth. 
The feature works in a very similar way with the the first feature.
"""
class TopResource(object):
    def __init__(self, fname):
        self.fname = fname
        self.dict = {}
        return
    
    def getItem(self, item):
        res = item.resource;
        nbytes = item.size
        if res in self.dict:
            self.dict[res] += nbytes
        else:
            self.dict[res] = nbytes
        return    
        
    def output(self):
        heap = []
        heapq.heapify(heap)
        
        topList = []
        for res, count in self.dict.items():
            if len(heap) < 10:
                heapq.heappush(heap, (count, res))
            else:
                if count >= heap[0][0]:
                    heapq.heappop(heap)
                    heapq.heappush(heap, (count, res))
        while heap:
            count, res = heapq.heappop(heap)
            topList.append(res)
        f = open(self.fname, 'w')
        while topList:
            currline = topList.pop()
            f.write(currline + "\n")
        f.close()
        return

"""
Feature 3: Get top 10 busiest 60 min time window
"""                
class BusyTime(object):
    def __init__(self, fname):
        self.fname = fname
        self.dq = deque([]) #deque to save {time, count, timstamp} for at most an hour
        self.heap = [] #heap for top 10 as {count, -tm_sec, timestamp}
        self.total = 0 #  visits within each 60 min window,
        heapq.heapify(self.heap)
        
    def getItem(self, item):
        tm_str = item.time
        tm_sec = item.stamp
        #remove item outside of 3600 sec time window
        while self.dq and (tm_sec - self.dq[0][0]) >= 3600:
            if len(self.heap) < 10:
                heapq.heappush(self.heap, (self.total, -self.dq[0][0], self.dq[0][2])) #negative value to ensure when different window has the same visits time, we can list the time in ascending order
            else:
                if self.total > self.heap[0][0]:
                    heapq.heappop(self.heap)
                    heapq.heappush(self.heap, (self.total, -self.dq[0][0], self.dq[0][2]))        
                    
            self.total -= self.dq[0][1]
            self.dq.popleft()
        if self.dq and self.dq[-1][0] == tm_sec:
            t, c, s = self.dq.pop()
            self.dq.append((t, c + 1, s))
        else:
            while self.dq and self.dq[-1][0]!= tm_sec -1:
                new_tm = self.dq[-1][0] + 1
                new_tm_str = timeReverse(new_tm)
                self.dq.append((new_tm, 0, new_tm_str))
            new_tm, new_tm_str = tm_sec, timeReverse(tm_sec)
            self.dq.append((new_tm, 1, new_tm_str))           
        self.total += 1    
        return
        
    def output(self):
        while self.dq:            
            if len(self.heap) < 10:
                heapq.heappush(self.heap, (self.total, -self.dq[0][0], self.dq[0][2]))
            else:
                if self.total > self.heap[0][0]:
                    heapq.heappop(self.heap)
                    heapq.heappush(self.heap, (self.total, -self.dq[0][0], self.dq[0][2]))      
            self.total -= self.dq[0][1]
            self.dq.popleft() 

        f = open(self.fname, 'w')    
        topList = []        
        while self.heap:
            count, sec, time = heapq.heappop(self.heap)
            topList.append(time + "," + str(count))
                
        while topList:
            currline = topList.pop()
            f.write(currline + "\n")
        f.close()
        return

"""
Feature 4: 
Detect patterns of three consecutive faileid login attempts over 20 seconds 
and block all further attempts to reach the site from the same IP address 
for the next 5 minutes.
"""                 
class BlockList(object):
    def __init__(self, fname):
        self.f = open(fname, 'w')
        
        self.dqBlock = deque([]) 
        self.blockList = {}
        
        self.dqFail = deque([]) #<time, hash<ip, cnt_in_this_second> >
        self.failCounter = {}
        
        
    def getAndOutput(self, item):
        ip = item.IP
        line = item.line
        time = item.stamp
        sta = item.status
        if self.checkBlock(ip, time):
            self.f.write(line)
        else:
            if sta:
                self.FailReset(ip)
            else:
                self.UpdateFail(ip, time)
        return
    
    """
    1) Update dqBlock and blocklist by removing items older than 300 secs
    2) Check if new ip stays in blocklist
    """
    def checkBlock(self, ip, time):
        #update current block list and check if ip is in the list
        while self.dqBlock and time - self.dqBlock[0][0] >= 300:
            del self.blockList[self.dqBlock[0][1]]
            self.dqBlock.popleft()
        return ip in self.blockList
    
    """
    Update dqFail and failCounter when the coming login is success
    """
    def FailReset(self, ip):
        #reset the 20s fail count after a successful login
        if ip not in self.failCounter:
            return
        else:
            del self.failCounter[ip]
            for i in range(len(self.dqFail)):
                if ip in self.dqFail[i][1]:
                    del self.dqFail[i][1][ip]
        return
    
    """
    Called only when the coming login fails.
    1) Update dqFail and failCounter by removing record older than 20 secs
    2) Put the coming login into failCounter and dqFail
    3) Check if the coming IP has failed 3 times
    """
    def UpdateFail(self, ip, time):
        
        #Update dqFail and failCounter by removing items older than 20 sec
        while self.dqFail and time - self.dqFail[0][0] >= 20:
            dict = self.dqFail[0][1]
            for i, c in dict.items():
                self.failCounter[i] -= c
                if self.failCounter[i] == 0:
                    del self.failCounter[i]
            self.dqFail.popleft()        
            
        #Set new ip into dqFail and failCounter
        if self.dqFail and time == self.dqFail[-1][0]:
            dict = self.dqFail[-1][1]
            if (ip not in dict):
                dict[ip] = 1
            else:
                dict[ip] += 1
        else:
            self.dqFail.append((time, {ip : 1}))
    
        if (ip not in self.failCounter):
            self.failCounter[ip] = 1
        else:
            self.failCounter[ip] += 1
            
        #Update blocklist and dqBlock if necessary
        if 3 == self.failCounter[ip]:
            self.dqBlock.append((time, ip))
            self.blockList[ip] = 1
        return   
        
    def finalize(self):
        f.close()
        return 
    
"""
main program
"""
input_list = str(sys.argv)
print 'argument list', input_list
logfile = sys.argv[1];
hostfile = sys.argv[2]
resfile = sys.argv[3]
hourfile = sys.argv[4]
blockfile = sys.argv[5]
print 'input file is: ', logfile
print 'host file is: ', hostfile
print 'resource file is: ', resfile
print 'hour file is: ', hourfile
print 'block file is: ', blockfile
f = open(logfile)

#Initialize all features
print 'Initial all features...'
ti = TopIP(hostfile)
tr = TopResource(resfile)
bt = BusyTime(hourfile)
bl = BlockList(blockfile)

print 'Start to process all given logs... '
cnt = 0
for line in f:
    item = parse(line)
    if not item:
        print 'Warning: Invalid log line encountered on line #:', line
        continue
    #item.print_out() #For debug printout
    ti.getItem(item)
    tr.getItem(item)
    bt.getItem(item)
    bl.getAndOutput(item)
    cnt += 1
    if not cnt % 100:
        print cnt, 'logs has been processed.'

print 'Start to output all results... '
ti.output()
tr.output()
bt.output()
bl.finalize()
f.close()
print 'Program finishes succesfully!'
