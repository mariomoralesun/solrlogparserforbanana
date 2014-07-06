import re
import json
import requests
import argparse
import os
import datetime
import atexit

class SolrServer:
    databuffer = ''
    buffercount = 0
    sendinc = 5000
    
    def __init__(self,solrserver,collection,sendinc):

        self.solr = solrserver
        if solrserver[-1] == '/':
            self.solr = solrserver
        else:
            self.solr += '/'
        
        self.solr += str(collection) + '/'
        self.sendinc = sendinc
        
    def set_param(p,v):
        self[p] = v
            
    def log_out(self,s):
        print("{} - {}".format(datetime.datetime.now(),s)) 
    
    def send_dict_to_solr(self,data,commit):
        data = json.dumps(data)
        if self.buffercount == 0:
            self.databuffer = '[\n'
        self.buffercount += 1
        self.databuffer += data + ',\n'
        solr = self.solr
        if self.buffercount >= self.sendinc:
            if commit == 1 or commit == True:
                #solr = self.solr+'update?commit=true'
                solr = self.solr+'update'
                self.log_out("Sending Commit with next batch")
            else:
                solr = self.solr+'update'

            self.databuffer = self.databuffer[:-2]
            self.databuffer += ']\n'
            
            r = requests.post(solr,data=self.databuffer,headers = {'content-type': 'application/json'})
            if r.status_code != 200:
                self.log_out("ERROR - Couldn't Index Data Into Solr")
                self.log_out(r.status_code)
                self.log_out(r.raw)
                self.log_out(r.url)
                with open('error.log','w+') as er:
                    er.write(data)
                    er.close()
            else:
                self.log_out("Send Successful")
            r.close()
            self.databuffer = ''
            self.buffercount = 0
        return True
    
    def send_rest(self):
        self.buffercount = self.sendinc
        t = {}
        t['id'] = 'commit'
        self.send_dict_to_solr(t,1)
