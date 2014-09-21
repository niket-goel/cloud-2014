package org.myorg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
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

public class distributedwordcount {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private Path[] localFiles;
		private java.util.Map<String, String> wordPatternMap;

		public void configure(JobConf job) {
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
				FileReader patternFile = new FileReader(
						localFiles[0].toString());
				BufferedReader br = new BufferedReader(patternFile);
				
				char[] fileContents = new char[1000];
				patternFile.read(fileContents);
				patternFile.close();
				String fileString = fileContents.toString();
				wordPatternMap = new HashMap<String, String>();
				String[] words = fileString.split(" ");
				for (int i = 0; i < words.length; i++) {
					wordPatternMap.put(words[i], words[i]);
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String currentWord = tokenizer.nextToken();
				word.set(currentWord);
				if (wordPatternMap.containsKey(currentWord)) {
					output.collect(word, one);
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(distributedwordcount.class);
		conf.setJobName("distributedwordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		DistributedCache.addCacheFile(new URI("s3://niketsbucket/word-patterns.txt"), conf);

		JobClient.runJob(conf);
	}
}
