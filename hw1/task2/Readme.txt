This program computes the frequency of every pair of words in a given document using map-reduce.
The usage is the following : 

============================================================================================================================================
FutureGrid: 
#$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar WordCountDouble.jar wordcountdouble <input folder> <output folder>

A few things to keep in mind are:
1. Both the input and output files are assumed to be in HDFS. This means that before running the code the input files must be copied into HDFS and the ouput has to be copied out of HDFS to the local filesystem.
2. The program will not work if the <output folder> already exists.

=============================================================================================================================================
Amazon AWS:
After creating the cluster on EMR, you need to add a step to execute a custom jar. All the files are read from and written to an s3 bucket. The jar itself must also be placed in an s3 bucket. 
The exact location of the custom jar must be specified in the path-to-jar.

The arguments must be given as:
wordcountdouble s3://<bucket-name>/<path-to-input> s3://<bucket-name>/<path-to-output>

Again the same rules apply, i.e. the output folder MUST NOT already exist.

output links: 
https://s3.amazonaws.com/niketsbucket/output2/_SUCCESS
https://s3.amazonaws.com/niketsbucket/output2/part-00000
https://s3.amazonaws.com/niketsbucket/output2/part-00001
https://s3.amazonaws.com/niketsbucket/output2/part-00002
https://s3.amazonaws.com/niketsbucket/output2/part-00003
https://s3.amazonaws.com/niketsbucket/output2/part-00004
https://s3.amazonaws.com/niketsbucket/output2/part-00005
https://s3.amazonaws.com/niketsbucket/output2/part-00006
=============================================================================================================================================== 
