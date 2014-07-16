solrlogparserforbanana (https://github.com/wrdrvr/solrlogparserforbanana)
======================

Solr Log Parser for LucidWorks Banana (https://github.com/LucidWorks/banana)

Written for python 3.4 and Solr 4.9, requires python requests module. The purpose of these scripts is to parse Solr logs for user queries, massage the data and index it into a Solr collection that can be used for Banana Dashboard lookups. One thing to keep in mind, is that there are about a million ways to set up solr logs, so this may not work for everyone. Also, ensure that your logging settings are properly configured to log user queries. Also, if your datestamp is in epoch, it will not get indexed, I will fix this later as I have more time.  

You can run the parser in two modes, archive and tail mode. Archive mode is used to process a backlog of logs in it's own directory, you can specify the location with the -logs argument, it will take a directory of a specific file. One caveat is that the files need to be gzipped prior to running this. Archive mode uses readlines python method to fastest performance, however you need to have enough RAM to hold the entire log file. If you run into memory problems, you can see use tail mode to pull it in, which uses readline and is quite a bit slower. 

The second mode is tail mode, you can use this to tail active production log files and immediately index data into Solr. It also takes the -logs argument and allows you to specify a file or a directory where logs live. It will try to find the active file and attach itself to it. 

In both modes the script will create a tab delimited control file (parsercontrolfile-archive.txt for archive mode and parsercontrolfile-tail.txt for tail mode) in the -logs directory, ensure you have write access to it. This is used by the script to keep track of everything it has already completed. In archive mode, it will add a status of 0 for a file if it is in progress and status of 1 if it is completed. This allows you to run the same script multiple times on the same directory to parse many archive log files quicker. In tail mode, it will log the byte offset for the file, this allows it to resume parsing from the last known position in the file.

-sendinc - Determines how many items the script should send to Solr at any one time. Default is 5000, tweak this for your volume and speed. 
-commit - Allows you to send a commit after a specific number of items are indexed (sendinc). Generally, I would recommend you rely on Solr's AutoCommit functionality, but figured it may be useful for someone. 
-collection  - Destination Solr Collection (e.x collection1)
-solr - Solr instance (i.e. http://localhost:8983/solr)
-offset - Provides a way to specify time offset between the time in the logs and UTC. (i.e. -offset +5 for EST)

Example: 
python logparse2.py -tail -logs /logs/ -solr http://192.168.137.128:8983/solr -collection collection1 -sendinc 15000

	run logparser2.py -h for help and usage instructions:
	usage: logparse2.py [-h] [-solr [SOLR]] [-collection [COLLECTION]]
						[-sendinc [SENDINC]] [-commit [COMMIT]]
						[-logs LOGS [LOGS ...]] [-archive] [-tail]
						[-offset [OFFSET]]

	Script to Parse Solr Core log file for reporting using banana

	optional arguments:
	  -h, --help            show this help message and exit
	  -solr [SOLR]          Address of Solr server (ex:
							http://192.168.137.128:8983/solr/)
	  -collection [COLLECTION]
							Name of Collection: (ex: collection1)
	  -sendinc [SENDINC]    How many documents send to Solr in a single batch
							(default is 5000)
	  -commit [COMMIT]      Optional argument to define after how many documents
							to send a commit. Not recommended, use Solr AutoCommit
							instead.
	  -logs LOGS [LOGS ...]
							Location of the log files (ex: /logs/)
	  -archive              Use this to process log files in an archive, they have
							to be gzipped. Point at a directory and let it go.
	  -tail                 Use this option to tail active log files. Point it at
							solr's log dir and it will do the rest.
	  -offset [OFFSET]      Provide log time offset to UTC. For example, if in
							eastern use -offset +5



					
Configure the following fields in your schema: 
Schema Fields:

	<field name="QTime" type="int" indexed="true" stored="true"/>
	<field name="hits" type="int" indexed="true" stored="true"/>
	<field name="event_timestamp" type="date" indexed="true" stored="true"/>

	<field name="q" type="qtext" indexed="true" stored="true" termVectors="true" termPositions="true" termOffsets="true" />
	<field name="fq" type="qtext" indexed="true" stored="true" termVectors="true" termPositions="true" termOffsets="true" />
	<field name="sort" type="qtext" indexed="true" stored="true" termVectors="true" termPositions="true" termOffsets="true" />
	<dynamicField name="fq_*"  type="qtext"  indexed="true"  stored="true" />

	<dynamicField name="*"  type="string"  indexed="true"  stored="true" />
	
	
	FieldType:
		<fieldType name="qtext" class="solr.TextField" positionIncrementGap="100">
      <analyzer type="index">
        <tokenizer class="solr.StandardTokenizerFactory"/>
        <filter class="solr.LowerCaseFilterFactory"/>
      </analyzer>
      <analyzer type="query">
        <tokenizer class="solr.StandardTokenizerFactory"/>
        <filter class="solr.LowerCaseFilterFactory"/>
      </analyzer>
    </fieldType>
	
To Do List:
	Incorporate functionality that would read a configuration file and transform certain values for certain fields appropriately. This would be used to translate field names and values from codes into "management type" friendly names. 