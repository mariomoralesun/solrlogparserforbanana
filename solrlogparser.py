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
        out = {}
        final = {}
        temp = l.split(' ')
        if len(temp)>1:
            out['date'] = temp[0]
            out['time'] = temp[1]
            out['event_timestamp'] = "%sT%sZ" % (temp[0], temp[1])
            
            if len(temp)>7 and temp[3] == 'core.SolrCore':
                
                if temp[2] == "INFO" and temp[7] == 'path=/select':
                    if 'ids' in temp[8] or 'group.topgroups.gsin' in temp[8]:
                        return out
                    out['type'] = 'select'
                    out['collection'] = self.getCollection(temp[5])
                    out['hits'] = temp[9].replace('hits=','')
                    out['status'] = temp[10].replace('status=','')
                    out['qtime'] = temp[11].replace('QTime=','')
                    params = self.parseParams(temp[8])
                    
                    if 'q' in params:
                        out.update(params)
                        out['id'] = out['event_timestamp'] + '_' + out['q']
                    elif 'fq' in out:
                        out.update(params)
                        out['id'] = out['event_timestamp'] + '_' + out['fq']
                    
                    if 'id' in out:
                        out['id'] = out['id'].replace(':','_')
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
            la = l.split('=')
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
        return out
  
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
