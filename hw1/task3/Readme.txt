This program computes the frequency of every word present in a given document(A) in another document(B) using map-reduce.
The usage is the following : 

============================================================================================================================================
FutureGrid: 
#$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar WordCountCache.jar distributedwordcount <input folder A> <output folder>

A few things to keep in mind are:
1. Both the input and output files are assumed to be in HDFS. This means that before running the code the input files must be copied into HDFS and the ouput has to be copied out of HDFS to the local filesystem.
2. The program will not work if the <output folder> already exists.
3. The file B is assumed to be in HDFS in the root directory and the file name should be 'word-patterns.txt'. This should be ensured before running the code. It is not made a command line argument for the sake of brevity.

=============================================================================================================================================
Amazon AWS:
After creating the cluster on EMR, you need to add a step to execute a custom jar. All the files are read from and written to an s3 bucket. The jar itself must also be placed in an s3 bucket. 
The exact location of the custom jar must be specified in the path-to-jar.

The arguments must be given as:
distributedwordcount s3://<bucket-name>/<path-to-input-A> s3://<bucket-name>/<path-to-output>

Again the same rules apply, i.e. the output folder MUST NOT already exist.

The file B is assumed to be in s3://niketsbucket/word-patterns.txt. Make sure you have the input text to be matched in a file by the same name under a bucket of the same name. Again this value is hardcoded for brevity.

=============================================================================================================================================== 
