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
Schema Fields:
   <field name="QTime" type="int" indexed="true" stored="true"/>
   <field name="hits" type="int" indexed="true" stored="true"/>
   <field name="event_timestamp" type="date" indexed="true" stored="true"/>
   <field name="q" type="qtext" indexed="true" stored="true" termVectors="true" termPositions="true" termOffsets="true" />
   <field name="fq" type="qtext" indexed="true" stored="true" termVectors="true" termPositions="true" termOffsets="true" />
   <field name="sort" type="qtext" indexed="true" stored="true" termVectors="true" termPositions="true" termOffsets="true" />
   
	<dynamicField name="fq_*"  type="qtext"  indexed="true"  stored="true" />
	<dynamicField name="sort_*"  type="qtext"  indexed="true"  stored="true" />
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