import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class PageRank {

    public static class PreProcessMap extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            int neighborCount = 0;
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            // The first token will be the node
            neighborCount += tokenizer.countTokens() - 1;

            // If we use a single output key, only one mapper will have to do everything,
            // so dividing the keys based on the tasks. Cannot break further as the avg outdegree
            // can be calculated only after finding the maximum nodes and edges.
            output.collect(new Text("AVG"), new IntWritable(neighborCount));
            output.collect(new Text("MAX"), new IntWritable(neighborCount));
            output.collect(new Text("MIN"), new IntWritable(neighborCount));
        }

    }

    public static class PreProcessReduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            int nodeCount = 0;
            int edgeCount = 0;
            int maxOutdegree = 0;
            int minOutdegree = Integer.MAX_VALUE;
            int avgOutdegree = 0;
            switch (key.toString()) {
                case "AVG":

                    while (values.hasNext()) {
                        nodeCount++;
                        edgeCount += Integer.parseInt(values.next().toString());
                    }
                    output.collect(new Text("Node Count"), new IntWritable(
                            nodeCount));
                    output.collect(new Text("Edge Count"), new IntWritable(
                            edgeCount));
                    avgOutdegree = nodeCount / edgeCount;
                    output.collect(new Text("Average outdegree"),
                            new IntWritable(avgOutdegree));
                    break;
                case "MAX":
                    while (values.hasNext()) {
                        int current = Integer.valueOf(values.next().toString());
                        if (current > maxOutdegree) {
                            maxOutdegree = current;
                        }
                    }
                    output.collect(new Text("Maximum outdegree"),
                            new IntWritable(maxOutdegree));
                    break;
                case "MIN":
                    while (values.hasNext()) {
                        int current = Integer.valueOf(values.next().toString());
                        if (current < minOutdegree) {
                            minOutdegree = current;
                        }
                    }
                    output.collect(new Text("Minimum outdegree"),
                            new IntWritable(minOutdegree));
                    break;

            }
        }

    }

    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, String> {

        public void map(LongWritable key, Text value,
                OutputCollector<Text, String> output, Reporter reporter)
                throws IOException {
            try {
                // damping factor
                float d = .85f;
                float pageRank = 1.0f;
                List<String> neighbors = new ArrayList<String>();

                String line = value.toString();
                String node;
                StringTokenizer tokenizer = new StringTokenizer(line, "\t");
                if (tokenizer.countTokens() > 1) {
                    node = tokenizer.nextToken();
                    if (tokenizer.hasMoreTokens()) {
                        StringTokenizer finerTokenizer =
                                new StringTokenizer(tokenizer.nextToken());
                        pageRank = Float.parseFloat(finerTokenizer.nextToken());
                        while (finerTokenizer.hasMoreTokens()) {
                            neighbors.add(finerTokenizer.nextToken());
                        }
                    }
                } else {
                    tokenizer = new StringTokenizer(line);
                    node = tokenizer.nextToken();
                    while (tokenizer.hasMoreTokens()) {
                        neighbors.add(tokenizer.nextToken());
                    }
                }

                for (String neighbor : neighbors) {
                    float newPageRank =
                            (1 - d) + d * (pageRank / neighbors.size());
                    output.collect(new Text(node), "__ " + neighbor + " __");
                    output.collect(new Text(neighbor), "" + newPageRank);
                }
            } catch (Exception e) {
                System.err.println(e.getMessage()
                        + "===========================================");
                e.printStackTrace();
            }

        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, String, Text, String> {
        public void reduce(Text key, Iterator<String> values,
                OutputCollector<Text, String> output, Reporter reporter)
                throws IOException {
            float pageRank = 0;
            String adjacencyListString = new String();
            while (values.hasNext()) {
                String value = values.next();
                if (value.contains("__")) {
                    // It is a node
                    StringTokenizer tokenizer = new StringTokenizer(value);
                    tokenizer.nextToken();
                    adjacencyListString += " " + tokenizer.nextToken();

                } else {
                    // It is a page rank
                    pageRank += Float.parseFloat(value);
                }
            }
            output.collect(key, pageRank + adjacencyListString);
        }
    }

    public static class PostProcessMap extends MapReduceBase implements
            Mapper<LongWritable, Text, FloatWritable, String> {

        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<FloatWritable, String> output, Reporter reporter)
                throws IOException {
            StringTokenizer tokenizer =
                    new StringTokenizer(value.toString(), "\t");
            String node;
            String pageRank;
            if (tokenizer.hasMoreTokens()) {
                node = tokenizer.nextToken();
                if (tokenizer.hasMoreTokens()) {
                    StringTokenizer finerTokenizer =
                            new StringTokenizer(tokenizer.nextToken());
                    pageRank = finerTokenizer.nextToken();
                    output.collect(new FloatWritable(Float.parseFloat(pageRank)), node);
                }
            }
        }

    }

    public static class PostProcessReduce extends MapReduceBase implements
            Reducer<FloatWritable, String, Text, String> {

        @Override
        public void reduce(FloatWritable key, Iterator<String> values,
                OutputCollector<Text, String> output, Reporter reporter)
                throws IOException {
            while (values.hasNext()) {
                output.collect(new Text(values.next()), key.toString());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        // Configuring the Hadoop job parameters
        int loop_count = 1;

        // First generate the metadata for the graph
        Path input, output;

        input = new Path(args[0]);
        output = new Path(args[1] + "/Metadata/");

        // Parse inputs to extract node topology information
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("GraphMetadata");
        conf.setMapperClass(PreProcessMap.class);
        conf.setReducerClass(PreProcessReduce.class);

        conf.set(
                "io.serializations",
                "org.apache.hadoop.io.serializer.JavaSerialization,"
                        + "org.apache.hadoop.io.serializer.WritableSerialization");
        conf.set("mapred.textoutputformat.separator", " ");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.set("mapred.textoutputformat.separator", "\t");


        FileInputFormat.setInputPaths(conf, input);
        FileOutputFormat.setOutputPath(conf, output);

        JobClient.runJob(conf);

        input = new Path(args[0]);
        output = new Path(args[1]+"/Output" + loop_count + "/");

        while (loop_count < 16) {
            loop_count++;
            conf = new JobConf(PageRank.class);
            conf.setJobName("pagerank");

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(String.class);

            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            conf.set(
                    "io.serializations",
                    "org.apache.hadoop.io.serializer.JavaSerialization,"
                            + "org.apache.hadoop.io.serializer.WritableSerialization");


            FileInputFormat.setInputPaths(conf, input);
            FileOutputFormat.setOutputPath(conf, output);

            JobClient.runJob(conf);
            /**
             * The output of one iteration is the input to the next iteration
             */
            input = new Path(args[1]+"/Output" + (loop_count - 1) + "/");
            output = new Path(args[1]+"/Output" + loop_count + "/");
        }

        // Sorting the values according to page rank
        input = new Path(args[1]+"/Output" + (loop_count - 1) + "/");
        output = new Path(args[1]+"/Output_sorted/");

        conf = new JobConf(PageRank.class);
        conf.setJobName("PageRankSorted");
        conf.setMapperClass(PostProcessMap.class);
        conf.setReducerClass(PostProcessReduce.class);

        conf.set(
                "io.serializations",
                "org.apache.hadoop.io.serializer.JavaSerialization,"
                        + "org.apache.hadoop.io.serializer.WritableSerialization");
        conf.set("mapred.textoutputformat.separator", " ");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(String.class);
        
        conf.setMapOutputKeyClass(FloatWritable.class);
        conf.setMapOutputValueClass(String.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // Force reducer to be one, in order to sort the ranks
        conf.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(conf, input);
        FileOutputFormat.setOutputPath(conf, output);

        JobClient.runJob(conf);

    }
}
