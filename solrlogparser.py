import re
import json
import requests
import argparse
import os

class SolrLogParser:
    def __init__(self):
        pass
    
    def log_out(s):
        print("{} - {}".format(date.datetime.now(),s))
    
    def parseSolrCoreLine(self,l):
        #Do string filtering for single, double quotes here. As well as for any unencoded characters
        out = {}
        final = {}
        temp = l.split(' ')
        if len(temp)>3:
            out['date'] = str(temp[0])
            out['time'] = str(temp[1])
            out['event_timestamp'] = str("{}T{}Z".format(temp[0], temp[1]))
            if 'b' in out['event_timestamp']:
                out['event_timestamp'] = out['event_timestamp'][2:]
                
            if len(temp)>7 and temp[3] == 'core.SolrCore' and temp[2] == "INFO":
                accepted = ['path','status','QTime','hits']
                out['collection'] = self.getCollection(temp[5])
                for item in temp[6:len(temp)]:
                    #Item is each pair of arguments, ex: status=0
                    if re.search('.*=.*',item):
                        #Only proceed if it has an equal sign in it and it is not the params field
                        t2 = item.split('=')
                        for param in t2:
                            if t2[0] in accepted:
                                out[t2[0]] = t2[1].replace('/','')
                    
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
        d = d.replace('{','')
        d = d.replace('}','')
        d = d.replace('"','')
        d = d.replace('params=','')
        out = {}
        t= d.split('&')
        count = 0
        for l in t:
            try:
                #l is each set of arguments
                la = l.split('=')
                #la is an array of parameter and value
                if la[0] == 'fq':
                    out['fq'] = ''
                    lb = la[1].split(':')
                    out['fq_'+ lb[0]] = lb[1]
                    out['fq'] += lb[0] + ' '
                else:
                    if ':' in la[1]:
                        la[1] = la[1].replace(':','=')
                        out[la[0]] = la[1]
                    else:
                        out[la[0]] = la[1]
            except:
                print("Problem is Parse Params, dump below:")
                print(l)
                print(out)
                
        return out
        
    def filter_data(self,d):
        a = {}
        blocked = ['shard.url','lowercaseOperators','distrib','mm','isShard','f.mmfr_exact.facet.limit','defType','group.ngroups','group','wt','timeAllowed','facet.mincount','fl','boost','facet.threads','ps','f.cat.facet.prefix','qf','group.sort','ids','fq_sas','group.facet','f.attr.facet.limit','group.facet','group.facet''facet']
        for x in d:
            if not x in blocked:
                a[x] = d[x]
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
        type = ''
        if re.search(r'core.\d\d\d\d_\d\d_\d\d.log.gz$',filename):
            type='solrcore'
        return type
        
    def logtype_uncomp(self, filename):
        type = ''
        if re.search(r'core.\d\d\d\d_\d\d_\d\d.log$',filename):
            type='solrcore'
        return type        
