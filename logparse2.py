import re
import json
import requests
import argparse
import os
import sys
from solrlogparser import SolrLogParser
import datetime
from SolrServer import SolrServer
import gzip 
import time

parser = argparse.ArgumentParser(description='Script to Parse Solr Core log file for reporting using banana')
parser.add_argument('-solr', type=str, default='http://localhost:8983/solr/', nargs='?', help='Address of Solr server (ex: http://192.168.137.128:8983/solr/)')
parser.add_argument('-collection', type=str, nargs='?', help='Name of Collection: (ex: collection1)')
parser.add_argument('-sendinc', default=5000, type=int, nargs=1, help='How many documents send to Solr in a single batch (default is 5000)')
parser.add_argument('-commit', type=int, nargs='?', help='Optional argument to define after how many documents to send a commit. Not recommended, use Solr AutoCommit instead.')
parser.add_argument('-logs',  type=str, nargs='+', help='Location of the log files (ex: /opt/sw/solr/logs/)')
parser.add_argument('-archive',action='store_true', default=False, help='Use this to process log files in an archive, they have to be gzipped. Point at a directory and let it go.')
parser.add_argument('-tail',action='store_true', default=False, help='Use this option to tail active log files. Point it at solr\'s dir and it will do the rest. ')
parser.add_argument('-offset',type=str, default="+6", nargs='?', help='Provide log time offset to UTC. For example, if in eastern use -offset +5')
#Will add support for this later
#parser.add_argument('-s',action='store_true', default=False, help='Use this option to silence regular reporting. ')
args = parser.parse_args()

if len(sys.argv) < 2:
    os.system(sys.argv[0] + " -h")
    sys.exit()

maindata={}

control = {}

def log_out(s):
    print("{} - {}".format(datetime.datetime.now(),s))

def main():

    if args.archive:
        log_out("Running in archive Processing Mode")
        for log in args.logs:
            if os.path.isfile(log):
                #It's a file
                if re.search('core\.\d\d\d\d.*.log.gz$',log):
                    maindata['controlfile'] = os.path.dirname(os.path.realpath(log)) + os.sep + 'parsercontrolfile-archive.txt'
                    stat = archive_file_proc(log)
                    if stat == False:
                        log_out("Something Went Wrong with Processing " + log)
                else:
                    log_out(log + " Doesn't meet the criteria, files need to be compressed, ending in .log.gz")

            elif os.path.isdir(log):
                #It's a directory
                log_out("Going to Process {} in archive mode".format(log))
                files = doDir(log)
                if len(files) > 0:
                    if log[-1] != os.sep:
                        log += os.sep
                    maindata['controlfile']=log+'parsercontrolfile.txt-archive.txt'
                    for file in files:
                        log_out("Found "+ file)
                    for file in files:
                        stat = archive_file_proc(file)
                        if stat == False:
                            log_out("Something Went Wrong with Processing " + log)
            else:
                log_out("Supplied Input is not a valid directory or a compressed log file")

    elif args.tail:
        log_out("Running in -tail mode")
        if not os.path.isfile(args.logs[0]) and not os.path.isdir(args.logs[0]):
            log_out("Supply -logs to specify currently active log file or it's directory. Put in nohup and send to background ('&') in Linux")    
            sys.exit()
        else:
            maindata['controlfile'] = os.path.dirname(os.path.realpath(args.logs[0])) + os.sep + 'parsercontrolfile-tail.txt'
            if os.path.isfile(args.logs[0]):
                tail_file(args.logs[0])
            elif os.path.isdir(args.logs[0]):
                find_active_log(args.logs[0])
            else:
                log_out("Not a valid file specified")
    else:
        log_out("No Mode Specified, pick -archive for gzipped files, or -tail for current log files")

def find_active_log(dir):
    if not os.path.isdir(dir):
        log_out("Not a valid directory supplied")
    else:
        parser = SolrLogParser(args.offset)
        log_out("Checking {} for Active Log Files".format(dir))
        dirfilelist = [os.path.join(dir,f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir,f))]
        files = {}
        for file in dirfilelist:
            if parser.logtype_uncomp(file) == 'solrcore':
                files[file] = {'filename':os.path.basename(file),'size':os.path.getsize(file),'mtime':os.path.getmtime(file),'active':0}
                log_out("Found " +file)
        time.sleep(15)
        for file in files:
            if os.path.getsize(file) > files[file]['size'] and os.path.getmtime(file) > files[file]['mtime']:
                tail_file(file)
        time.sleep(45)
        for file in files:
            if os.path.getsize(file) > files[file]['size'] and os.path.getmtime(file) > files[file]['mtime']:
                tail_file(file)

def tail_file(file):
    parser = SolrLogParser(args.offset)
    solr = SolrServer(args.solr,args.collection,args.sendinc)
    
    if check_if_processed(file) == True:
        log_out("Already Processing " + file)
        return True
    else:
        if parser.logtype_uncomp(file) == 'solrcore':
            #mark_as_inprogress(file)
            log_out("Starting to Process " + file)
        else:
            log_out("File not in the correct name format " + file)
            return False
            
            
    startsize = os.path.getsize(file)
        
    with open(file,'r') as fh:
        filename = os.path.basename(file)
        if filename in control and (control[filename] != "0" or control[filename] != "1"):
            fh.seek(int(control[filename]))
            log_out("Resuming at " +control[filename])
        line = fh.readline()
        count = 0
        #left off here, need to set it up so it updates file byte offset in control file
        while line:
            data = {}
            data = parser.parseSolrCoreLine(str(line))
            if 'id' in data:
                
                count += 1
                if type(args.commit) == int and count % args.commit == 0:
                    print("Processed %s lines - Sending Data and commit to Solr" % (str(count)))
                    solr.send_dict_to_solr(data,1)
                    
                else: 
                    solr.send_dict_to_solr(data,0)

                if count % args.sendinc == 0:
                    mark_as_inprogress(file, fh.tell())
            line = fh.readline()
            
            if not line:
                log_out("Caught up on the File. Pending Changes - Short")
                time.sleep(10)
                if startsize < os.path.getsize(file):
                    startsize = os.path.getsize(file)
                    #File grew while we were processing it, so it is still active, need to periodically scan it. 
                    line = fh.readline
                    
            if not line:
                log_out("Caught up on the File. Pending Changes - Long")
                time.sleep(120)
                if startsize < os.path.getsize(file):
                    startsize = os.path.getsize(file)
                    #File grew while we were processing it, so it is still active, need to periodically scan it. 
                    line = fh.readline        
        mark_as_inprogress(file, fh.tell())
        fh.close()
        solr.send_rest()
        log_out("Finished with "+file)
    find_active_log(os.path.dirname(file))
    
def doDir(directory):
    files = [os.path.join(directory,f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory,f)) and re.search('core.\d\d\d\d.*.log.gz$',f)]
    if files:
        return files
    else:
        print("No files in the directory that end with .log.gz")
        return files
                
def archive_file_proc(file):
    parser = SolrLogParser(args.offset)
    solr = SolrServer(args.solr,args.collection,args.sendinc)
    
    if check_if_processed(file) == True:
        log_out("Already Processed " + file)
        return True
    else:
        if parser.logtype_comp(file) == 'solrcore':
            mark_as_inprogress(file)
            log_out("Starting to Process " + file)
        else:
            return False
    
    with gzip.open(file,'r') as fh:
        count = 1
        for line in fh.readlines():
            data = {}
           
            data = parser.parseSolrCoreLine(str(line))
            if 'id' in data:
                count += 1
                if type(args.commit) == int and count % args.commit == 0:
                    print("Processed %s lines - Sending Data to Solr" % (str(count)))
                    solr.send_dict_to_solr(data,1)
                    #write_json(file,data)
                else: 
                    solr.send_dict_to_solr(data,0)
                    #write_json(file, data)
        fh.close()
        solr.send_rest()
        mark_as_processed(file)
        return True

def write_json(file, data):
    directory = os.path.dirname(os.path.realpath(file))
    pass
    
    
def check_if_processed(file):
    global control
    file = os.path.basename(file)
    control = read_control()
    #print(control)
    log_out("Checking {}".format(file))
    
    try:
        if file in control and (file[control] == "1" or file[control] == "0"):
            return True
    except:
        return False

def mark_as_processed(file):
    global control
    control = read_control()
    file = os.path.basename(file)
    control[file] = "1"
    log_out("Marking {} as Processed".format(file))
    write_control()
    
    
def mark_as_inprogress(file,*stat):
    global control
    
    if stat:
        stat = str(stat[0])
    else:
        stat = "0"
    control = read_control()
    file = os.path.basename(file)
    control[file] = stat
    #log_out("Marking {} as In-Progress".format(file))
    write_control()
    
def write_control():
    if os.path.isfile(maindata['controlfile']):
        try:
            with open(maindata['controlfile'],'w') as maindata['pcf']:
                data = ''
                for file in control:
                    data += "{}\t{}\n".format(file,control[file])
                maindata['pcf'].write(data)
                maindata['pcf'].flush()
                maindata['pcf'].close()
        except:
            print("Couldn't Open Control File for Writing: " + maindata['controlfile'])
            sys.exit()
    
def read_control():
    global control
    control = {}
    if os.path.isfile(maindata['controlfile']):
        try:
            with open(maindata['controlfile'],'r+') as maindata['pcf']:
                for line in maindata['pcf'].readlines():
                    if line[:-1] == '\n':
                        a = line[:-1].split('\t')
                    else: 
                        a = line.split('\t')
                    control[a[0]] = a[1][0:-1]
                    maindata['pcf'].close()
            return control
        except:
            log_out("Couldn't Open Control File for Reading: " + maindata['controlfile'])
            sys.exit()
    else:
        try:
            maindata['pcf'] = open(maindata['controlfile'],'w+')
            maindata['pcf'].flush()
            maindata['pcf'].close()
            return control
        except:
            log_out("Couldn't Create Control File: " + maindata['controlfile'])
            sys.exit()


main()
