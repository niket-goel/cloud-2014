package org.myorg;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class wordcountdouble {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String wordOne;
			String wordTwo;
			String completeToken;
			StringTokenizer tokenizer = new StringTokenizer(line);
			try {
				/*
				 * The following code increments the tokenizer twice and also
				 * sets completeToken as the concatenation of two words that are
				 * separated by a " " delimiter. The mapper passes these
				 * concatenated words as the key to the reducers
				 */
				wordOne = tokenizer.nextToken();
				while (tokenizer.hasMoreTokens()) {

					if (tokenizer.hasMoreElements()) {
						wordTwo = tokenizer.nextToken();
					} else {
						wordTwo = "";
					}
					completeToken = wordOne + " " + wordTwo;
					wordOne = wordTwo;
					word.set(completeToken);
					output.collect(word, one);
				}
			} catch (Exception e) {
				System.err
						.print("An exception occurred. Probably the document was empty :::"
								+ e.getMessage());
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// The reducer adds up the occurrences of a particular word.
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		// Configuring the Hadoop job parameters
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		/*
		 * Reading input parameters. The parameters used are args[1] and args[2]
		 * as the class name itself is one parameter for the jar
		 */

		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
	}
}
