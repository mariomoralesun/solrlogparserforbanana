import re
import json
import requests
import argparse
import os

parser = argparse.ArgumentParser(description='Script to Parse Solr Core log file for reporting using banana')
parser.add_argument('-solr', type=str, nargs='+', help='Address of Solr server (ex: http://192.168.137.128:8983/solr/)')
parser.add_argument('-collection',  type=str, nargs='+', help='Name of Collection: (ex: collection1)')
parser.add_argument('-sendinc', type=int, nargs=1, help='Number of documents after which to send and commit (ex: 1000)')
parser.add_argument('-logs',  type=str, nargs='+', help='Directory of Log Files (ex: /opt/sw/solr/logs/)')
parser.add_argument('-workdir', default='./',type=str, nargs='+', help='Working Directory (ex: /opt/sw/solr/logs/)')
parser.add_argument('-tail',type=int, nargs='?', help='How many seconds to wait to parse files (ex: 60)')
args = parser.parse_args()


#C:\Python34\python.exe logparse.py -solr http://192.168.137.128:8983/solr/ -collection collection1 -sendinc 100 -log #C:\Users\nvasilyev\Downloads\core.2014_04_09-10k.log

maindata = ''
maincount = 0
mainparsedfiles = {}
mainparsecontrolfile = ''

def logtype(filename):
    type = ''
    if "core" in filename:
        type='solrcore'
    return type

def main():
    global mainparsecontrolfile
    readControl()
    if os.path.isfile(args.logs[0]):
        #Process as a file
        parseFile(args.logs[0])
    else:
        #Process Logs Directory
        doDir(args.logs[0])


def readControl():
    
    if args.workdir[0]:
        if os.path.isfile(args.workdir[0]+'logparser.txt'):
            try:
                mainparsecontrolfile = open(args.workdir[0]+'logparser.txt','r+')
            except:
                print("Couldn't Open Control File for Reading: " + args.workdir[0]+'logparser.txt')
        else:
            try:
                mainparsecontrolfile = open(args.workdir[0]+'logparser.txt','r+')
            except:
                print("Couldn't Create Control File: " + args.workdir[0]+'logparser.txt')
    global mainparsedfiles
    #global mainparsecontrolfile
    for x in mainparsecontrolfile.readlines():
        a = x[:-1].split('\t')
        mainparsedfiles[a[0]] = a[1]
    mainparsecontrolfile.close()
    return True
    
def writeControl():
    #global mainparsecontrolfile
    global mainparsedfiles
    if args.workdir[0]:
        if os.path.isfile(args.workdir[0]+'logparser.txt'):
            try:
                mainparsecontrolfile = open(args.workdir[0]+'logparser.txt','w+')
                readControl()
            except:
                print("Couldn't Open Control File: " + args.workdir[0]+'logparser.txt')
        else:
            try:
                mainparsecontrolfile = open(args.workdir[0]+'logparser.txt','w+')
                readControl()
            except:
                print("Couldn't Create Control File: " + args.workdir[0]+'logparser.txt')
    data = ''
    for x in mainparsedfiles:
        data += str(x) + "\t" + str(mainparsedfiles[x]) + "\n"
    mainparsecontrolfile.write(data)
    
    
def doDir(directory):
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory,f)) and re.search('.log$',f)]
    if files:
        for x in files:
            print("Parsing " +args.logs[0]+x)
            parseFile(args.logs[0]+x)
    else:
        print("No files in the directory that end with .log")
    
    
def parseFile(filename):
    global mainparsedfiles
    count = 0
    type1 = logtype(filename)
    try:
	    file = open(filename,'r')
    except:
	    print("Couldn't open File: " + filename)
    if filename in mainparsedfiles:
        file.seek(0,2)
        if str(file.tell()) == mainparsedfiles[filename]:
            print("File is not changed: " + filename + " - " + str(file.tell()))
            file.close()
            return True
        else:
            file.seek(int(mainparsedfiles[filename]),0)
    
    if type1 == 'solrcore':
        for l in file.readlines():
            t = parseSolrCoreLine(l)
            if 'id' in t:
                count +=1
                print ("Indexing Line %s" % (count))
                t['id'] = t['id'].replace(':','_')
                if count%args.sendinc[0] == 0:
                    indexToSolr(t,1)
                    pass
                else: 
                    indexToSolr(t,0)
                    pass
            mainparsedfiles[filename] = file.tell()
    file.close()
    writeControl()
    
def writeToFile(h):
    output = open(r'c:\out10k.json','w')
    j = json.dumps(h)
    output.write(j+'\n')
    output.close()

def indexToSolr(f,c):
    d = json.dumps(f)
    global maincount
    global maindata
    
    print ("Main Count is " + str(maincount))
    if c == 1:
        print("Sending Commit")
        solr = args.solr[0]+args.collection[0]+'/update?commit=true'
    else:
        print("Sending Data")
        solr = args.solr[0]+args.collection[0]+'/update'
    
    if maincount == 0:
        maindata = '[\n'
    maincount += 1
    maindata += d + ',\n'
    
    if maincount == args.sendinc[0]:
        maindata = maindata[:-2]
        maindata += ']\n'
        #print(maindata)
        r = requests.post(solr,data=maindata,headers = {'content-type': 'application/json'})
        r.close()
        maindata = ''
        maincount = 0


def parseSolrCoreLine(l):
	#Do string filtering for single, double quotes here. As well as for any unencoded characters
    out = {}
    final = {}
    temp = l.split(' ')
    #printarray(temp)
    if len(temp)>1:
        out['date'] = temp[0]
        out['time'] = temp[1]
        out['event_timestamp'] = "%sT%sZ" % (temp[0], temp[1])
        
        if len(temp)>7 and temp[3] == 'core.SolrCore':
            
            if temp[2] == "INFO" and temp[7] == 'path=/select':
                if 'ids' in temp[8] or 'group.topgroups.gsin' in temp[8]:
                    return out
                out['type'] = 'select'
                out['collection'] = getCollection(temp[5])
                out['hits'] = temp[9].replace('hits=','')
                out['status'] = temp[10].replace('status=','')
                out['qtime'] = temp[11].replace('QTime=','')
                params = parseParams(temp[8])
                
                if 'q' in params:
                    out.update(params)
                    out['id'] = out['event_timestamp'] + '_' + out['q']
                elif 'fq' in out:
                    out.update(params)
                    out['id'] = out['event_timestamp'] + '_' + out['fq']
    return out    

def parseParams(d):
    d = d.replace('{','')
    d = d.replace('}','')
    d = d.replace('"','')
    d = d.replace('params=','')
    out = {}
    t= d.split('&')
    count = 0
    for l in t:
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
    return out
    

    
def getCollection(d):
    d = d.replace('[','')
    d = d.replace(']','')
    if re.search('_',d):
        a = re.match('^(.+)_.*_.*',d)
        d = a.group(1)
    return d

def printarray(a):
    for i in range (0,len(a)):
        print( "%r - %r " % (i, a[i]) )
    print("\n\n\n--------\n\n\n")

main()
