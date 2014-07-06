solrlogparserforbanana
======================

Solr Core Log Parser for Banana

Written for python 3.4 and Solr 4.9, requires python requests module. 

run logparser2.py -h for help and usage instructions:

usage: logparse2.py [-h] [-solr SOLR [SOLR ...]]

                    [-collection COLLECTION [COLLECTION ...]]
                    [-sendinc SENDINC] [-commit COMMIT]
                    [-logs LOGS [LOGS ...]] [-workdir WORKDIR [WORKDIR ...]]
                    [-archive] [-tail]

Script to Parse Solr Core log file for reporting using banana

optional arguments:
  -h, --help            show this help message and exit
  -solr SOLR [SOLR ...]
                        Address of Solr server (ex:
                        http://192.168.137.128:8983/solr/)
  -collection COLLECTION [COLLECTION ...]
                        Name of Collection: (ex: collection1)
  -sendinc SENDINC      Number of documents after which to send and commit
                        (ex: 1000)
  -commit COMMIT        Number of documents after which to send and commit
                        (ex: 1000)
  -logs LOGS [LOGS ...]
                        Directory of Log Files (ex: /opt/sw/solr/logs/)
  -workdir WORKDIR [WORKDIR ...]
                        Working Directory (ex: /opt/sw/solr/logs/)
  -archive              Use this to process log files in an archive, they have
                        to be gzipped
  -tail                 Use this to process log files in an archive, they have
                        to be gzipped
