import re
import json
import requests
import argparse
import os
import time

parser = argparse.ArgumentParser(description='Script to Parse Solr Core log file for reporting using banana')
parser.add_argument('-solr', type=str, nargs='+', help='Address of Solr server (ex: http://192.168.137.128:8983/solr/)')
parser.add_argument('-collection',  type=str, nargs='+', help='Name of Collection: (ex: collection1)')
parser.add_argument('-sendinc', type=int, default=1000, nargs=1, help='Number of documents after which to send and commit (ex: 1000)')
parser.add_argument('-commit', type=int, default=10000, nargs=1, help='Number of documents after which to send and commit (ex: 1000)')
parser.add_argument('-logs',  type=str, nargs='+', help='Directory of Log Files (ex: /opt/sw/solr/logs/)')
parser.add_argument('-workdir', default='./',type=str, nargs='+', help='Working Directory (ex: /opt/sw/solr/logs/)')
parser.add_argument('-tail',type=int, nargs='?', help='How many seconds to wait to parse files (ex: 60)')
parser.add_argument('-fast',type=int, nargs='?', help='Use for indexing files that are no longer changing. Can\'t Resume ')
parser.add_argument('-noindex',type=int, nargs='?', help='Use for indexing files that are no longer changing. Can\'t Resume ')
args = parser.parse_args()

maindata = ''
maincount = 0
mainparsedfiles = {}
mainparsecontrolfile = ''
currentfile = ''

print(args)
def logtype(filename):
    type = ''
    if "core" in filename and not 'request' in filename:
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
    global mainparsedfiles
    global mainparsecontrolfile
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
                print("Couldn't Create Control File for Reading: " + args.workdir[0]+'logparser.txt')
    
    #global mainparsecontrolfile
    if mainparsecontrolfile:
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
                print("Couldn't Create Control File for Write: " + args.workdir[0]+'logparser.txt')
    data = ''
    for x in mainparsedfiles:
        data += str(x) + "\t" + str(mainparsedfiles[x]) + "\n"
    mainparsecontrolfile.write(data)
    
    
def doDir(directory):
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory,f)) and re.search('.log$',f)]
    compressedfiles = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory,f)) and re.search('.log.gz$',f)]
    if files:
        for x in files:
            #print("Parsing " +args.logs[0]+x)
            parseFile(args.logs[0]+x)
        #if not args.fast:
            #doDir(directory)
            
    if compressedfiles:      
        print("Going to Process These Files: ")
        print(compressedfiles)
        for x in compressedfiles:
            os.system("gunzip " + args.logs[0]+x.replace('.gz',''))
            parseFile(args.logs[0]+x)
            os.system("gzip " + args.logs[0]+x)
    else:
        print("No files in the directory that end with .log")
    
    
def parseFile(filename):
    global mainparsedfiles
    global currentfile
    
    comp = 0
    if '.gz' in filename:
        comp = 1
        os.system("gunzip " + filename)
        filename = filename.replace('.gz','')
    currentfile = filename
    count = 0    
    type1 = logtype(filename)
    try:
	    file = open(filename,'r')
    except:
	    print("Couldn't open File: " + filename)
    pos = 0    
    print("Parsing File %s" % (filename))
    if filename in mainparsedfiles:
        pos = file.tell()
        file.seek(0,2)
        if file.tell() == int(mainparsedfiles[filename]):
            print("File is not changed: " + filename + " - " + str(file.tell()))
            file.close()
            return True
        else:
            file.seek(int(mainparsedfiles[filename]),0)
        
    if type1 == 'solrcore':
        if args.fast:
            print("Wee... Going to go fast")
            for l in file.readlines():  
                s = ''
                t = parseSolrCoreLine(l)
                if 'id' in t:
                    count +=1
                    #print ("Indexing Line %s " % (count))#- file pos: %s" % (count, file.tell()))
                    t['id'] = t['id'].replace(':','_')
                    if count % args.sendinc[0] == 0:
                        s = indexToSolr(t,1)
                    else: 
                        s = indexToSolr(t,0)
                    if s == False:
                        print("Error at %s" % (count))
            mainparsedfiles[filename] = file.tell()
            writeControl()
            os.system("gzip " + filename)
            mainparsedfiles[filename+'.gz'] = mainparsedfiles[filename]
            
        else:
            print("Going to Tail")
            startsize = os.path.getsize(filename)
            l = file.readline()
            done = 0
            while l and done != 1:
                s = ''
                #print(l)
                t = parseSolrCoreLine(l)
                if 'id' in t:
                    count +=1
                    print ("Indexing Line %s " % (count))
                    t['id'] = t['id'].replace(':','_')
                    mainparsedfiles[filename] = file.tell()
                    if count % args.commit[0] == 0:
                        s = indexToSolr(t,1)
                        writeControl()
                    else: 
                        s = indexToSolr(t,0)
                    if s == False:
                        print("Error at %s" % (count))
                l = file.readline()
                
                if not l:
                    nowsize = os.path.getsize(filename)
                    if nowsize > startsize:
                        time.sleep(10)
                        l = file.readline()
                    else:
                        done = 1

        t = {}
        t['id'] = 'commit'
        indexToSolr(t,1)
    file.close()
    os.system("gzip " + currentfile)
    
    os.system("gzip " + filename)
    writeControl()

    
def writeToFile(data):
    global currentfile
    orig = ''
    if 'json' not in currentfile:
        orig = currentfile
        currentfile = currentfile + ".json"
        if os.path.isfile(currentfile) and not orig in existmainparsedfiles:
            os.system("rm -f " + currentfile)
    output = open(currentfile,'a+')
    output.write(data+'\n')
    output.close()
def filterData(d):
    a = {}
    blocked = ['shard.url','lowercaseOperators','distrib','mm','isShard','f.mmfr_exact.facet.limit','defType','group.ngroups','group','wt','timeAllowed','facet.mincount','fl','boost','facet.threads','ps','f.cat.facet.prefix','qf','group.sort','ids','fq_sas','group.facet','f.attr.facet.limit','group.facet','group.facet''facet']
    for x in d:
        if not x in blocked:
            a[x] = d[x]
    return a
    
def indexToSolr(f,c):
    f = filterData(f)
    d = json.dumps(f)
    global maincount
    global maindata
    #print ("Main Count is " + str(maincount))
    if c == 1:
        #print("Sending Commit")
        solr = args.solr[0]+args.collection[0]+'/update?commit=true'
    else:
        #print("Sending Data")
        solr = args.solr[0]+args.collection[0]+'/update'
    
    if maincount == 0:
        maindata = '[\n'
    maincount += 1
    maindata += d + ',\n'
    out = True
    if maincount == args.sendinc[0]:
        maindata = maindata[:-2]
        maindata += ']\n'
        #print(maindata)
        if not args.noindex:
            r = requests.post(solr,data=maindata,headers = {'content-type': 'application/json'})
            if r.status_code != 200:
                print(r.status_code)
                print(r.raw)
                print(r.text)
                out =  False
                r.close()

        writeToFile(maindata)
        maindata = ''
        maincount = 0
    return out

def parseSolrCoreLine(l):
	#Do string filtering for single, double quotes here. As well as for any unencoded characters
    out = {}
    final = {}
    temp = l.split(' ')
    if len(temp)>3:
        out['date'] = temp[0]
        out['time'] = temp[1]
        out['event_timestamp'] = "%sT%sZ" % (temp[0], temp[1]) 
        if len(temp)>7 and temp[3] == 'core.SolrCore' and temp[2] == "INFO":
            accepted = ['path','status','QTime','hits']
            out['collection'] = getCollection(temp[5])
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
                    params = parseParams(item)
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

                
"""
if  and temp[7] == 'path=/select':
if 'ids' in temp[8] or 'group.topgroups.gsin' in temp[8]:
    return out
out['type'] = 'select'

out['hits'] = temp[9].replace('hits=','')
if 'status' in temp[10]:
    out['status'] = temp[10].replace('status=','')
    out['qtime'] = temp[11].replace('QTime=','')
elif 'status' in temp[11]:
    out['status'] = temp[11].replace('status=','')
    out['qtime'] = temp[12].replace('QTime=','')
    temp[8] += temp[9].replace('\n','')
params = 


"""
