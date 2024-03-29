import re
import json
import requests
import argparse
import os

class SolrLogParser:
    utc_offset = 0
    matches = {\
           "time": { "1": r"\d\d:\d\d\:\d\d[,|\.]\d*", \
                    "1-len": 50},
           "date": { "1":r"\d\d\d\d\-\d\d\-\d\d", \
                     "1-len": 25}, \
           "params": r"params={.*}", \
           "params-len":  200000,\
           "QTime":"QTime=(\d+)",\
           "QTime-len":5000,\
           "status":"status=(\d+)",\
           "status-len":5000\
           }
    def __init__(self, offset):
        self.utc_offset = offset
    
    def log_out(s):
        print("{} - {}".format(date.datetime.now(),s))
    
    def parseSolrCoreLine(self,l):
        if not re.search('params={.*}',l) and not re.search('QTime', l):
            return
        #Do string filtering for single, double quotes here. As well as for any unencoded characters
        out = {}
        temp = l.split(' ')
        if len(temp)>3:
            out['date'] = str(temp[0])
            out['time'] = str(temp[1]) 
            if len(temp)>7 and temp[3] == 'core.SolrCore' and temp[2] == "INFO":
                out['event_timestamp'] = self.time_to_utc(out['date'],out['time'],self.utc_offset)
                if 'b' in out['event_timestamp']:
                    out['event_timestamp'] = out['event_timestamp'][2:]
                acceptedstrings = ['path']
                acceptedints = ['status','QTime','hits']
                out['collection'] = self.getCollection(temp[5])
                for item in temp[6:len(temp)]:
                    #Item is each pair of arguments, ex: status=0
                    if re.search('.*=.*',item):
                        #Only proceed if it has an equal sign in it and it is not the params field
                        t2 = item.split('=')
                        for param in t2:
                            if t2[0] in acceptedstrings:
                                out[t2[0]] = t2[1].replace('/','')
                            elif t2[0] in acceptedints:
                                out[t2[0]] = int(t2[1].replace('/',''))
                    if re.search('^params',item):
                        #Parse Params
                        params = self.parseParams(item)
                        if 'q' in params:
                            out.update(params)
                            out['id'] = out['event_timestamp'] + '_' + out['q']
                        elif 'fq' in out:
                            out.update(params)
                            out['id'] = out['event_timestamp'] + '_' + out['fq']
        out = self.filter_data(out)
        return out
        
    def parseParams(self, d):
        for x in ['{','}','"','params=']:
            d=d.replace(x,'')
        out = {}
        out['fq']=''
        out['sort']=''
        t= d.split('&')
        count = 0
        
        for l in t:
            try:
                #l is each set of arguments
                la = l.split('=')
                #la is an array of parameter and value
                if la[0] == 'fq':# or la[0] == 'sort':
                    la[1] = re.sub('[()]','',la[1])
                    la[1] = re.sub('\+','_',la[1])
                    if ':' in la[1]:
                        lb = la[1].split(':')
                        out[la[0] +'_'+ lb[0]] = lb[1]
                        out[la[0]] += lb[0] + ' '
                else:
                    if ':' in la[1]:
                        la[1] = la[1].replace(':','\:')
                        if la[0] != 'q': 
                            la[1] = la[1].replace(' ','_')
                        out[la[0]] = la[1]
                    else:
                        la[1] = re.sub('\+','_',la[1])
                        out[la[0]] = la[1]
            except:
                print("Problem is Parse Params, dump below:")
                print(l)
                print(la)
                print(lb)
                print(out)
                
        #Final Filtering due to sloppy data extraction
        if len(out['fq']) < 1: del out['fq']
        if len(out['sort']) < 1: del out['sort']
        
        return out
        
    def filter_data(self,d):
        a = {}
        blocked = ['shard.url','lowercaseOperators','distrib','mm','isShard','f.mmfr_exact.facet.limit','defType','group.ngroups','group','wt','timeAllowed','facet.mincount','fl','boost','facet.threads','ps','f.cat.facet.prefix','qf','group.sort','ids','fq_sas','group.facet','f.attr.facet.limit','group.facet','group.facet','facet','f.ind.facet.limit','f.cat.facet.limit','tie','f.ba.facet.limit','fsv','NOW','f.em.facet.limit','pf']
        for x in d:
            if not x in blocked:
                a[x] = d[x]
                
        if 'q' in a: 
            a['q'] = a['q'].replace('%2B','')
        return a
        
    def getCollection(self,d):
        d = d.replace('[','')
        d = d.replace(']','')
        if re.search('_',d):
            a = re.match('^(.+)_.*_.*',d)
            d = a.group(1)
        return d

    def printarray(self, a):
        for i in range (0,len(a)):
            print( "%r - %r " % (i, a[i]) )
        print("\n\n\n--------\n\n\n")
        
    def logtype_comp(self, filename):
        #Will run a filtering criteria later
        type = ''
        if re.search(r'core.\d\d\d\d_\d\d_\d\d.log.gz$',filename):
            type='solrcore'
        return 'solrcore'
        
    def logtype_uncomp(self, filename):
        #Will run a filtering criteria later
        type = ''
        if re.search(r'core.\d\d\d\d_\d\d_\d\d.log$',filename):
            type='solrcore'
        return 'solrcore'        
    
    def time_to_utc(self, date,time,offset):
        out = ''
        date=str(date)
        time=str(time)
        Y,M,D = date.split('-')
        h,m,s = time.split(':')
        s,ms = s.split(',')
        nh = 0
        #try:
        if offset != 0:
            D = int(D)
            nh =  int(h) + eval(offset)

            if nh > 24:
                nh -= 24
                D += 1
            elif nh < 0:
                nh += 24
                D -= 1
            
            if D > 31 and M in ['01','03','05','07','08','10','12']:
                M = int(M) + 1
                D -= 31
            elif D > 30 and M in ['02','04','06','09','11']:
                M = int(M) + 1
                D -= 30
            else:
                M = int(M)
                
            if int(D) < 10:
                D = "0{}".format(D)
            else:
                D = str(D)
               
            if M < 10:
                M = "0{}".format(M)
            else:
                M = str(M)

            
        out = "{}-{}-{}T{}:{}:{},{}Z".format(Y,M,D,nh,m,s,ms)
        return out

    def parseAnyLine(self,line):
        if not re.search('params={.*}',line) and not re.search('QTime', line):
            return
        out = {}
        matches = self.matches
        
        for match in matches:
            name = match
            if type(matches[match]) == dict:
                for submatch in matches[match]:
                    if 'len' not in submatch: 
                        regex = matches[match][submatch]
                        offset = matches[match][submatch+ '-len']
                        val = self.match_element(line,regex,offset)
                        if val != False and val != None:
                            out[name] = val
            else:
                if 'len' not in match: 
                    regex = matches[match]
                    offset = matches[match+ '-len']
                    val = self.match_element(line,regex,offset)
                    if val != False and val != None:
                        out[name] = val
        if 'date' in out and 'time' in out:
            out['event_timestamp'] = self.time_to_utc(out['date'],out['time'],self.utc_offset)
            if 'b' in out['event_timestamp']:
                out['event_timestamp'] = out['event_timestamp'][2:]
        else:
            return
        if 'params' in out and 'q' in out['params']:
            params = self.parseParams(out['params'])
            if 'q' in params:
                out.update(params)
                out['id'] = out['event_timestamp'] + '_' + out['q']
            elif 'fq' in out:
                out.update(params)
                out['id'] = out['event_timestamp'] + '_' + out['fq']
            out = self.filter_data(out)
            #print(line)
            #print(out)
            return out
        
    def match_element(self,line,regex,length):
            m = re.compile(r'{}'.format(regex)).findall(line[0:length])
            if m: return m[0]
            else: return False
        