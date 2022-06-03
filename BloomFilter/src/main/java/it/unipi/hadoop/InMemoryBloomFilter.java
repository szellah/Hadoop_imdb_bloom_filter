package it.unipi.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InMemoryBloomFilter {
    public static class BloomFilterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        // reuse Hadoop's Writable objects
        private final IntWritable reducerKey = new IntWritable();
        private final Text reducerValue = new Text();

        @Override
        // it receives a string wrapped as a Text object that need to be parsed in a
        // timeSeries object
        // returns a Text
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // To be implemented

            String s = value.toString();
            if (s == null || s.length() == 0)
                return;

            String[] tokens = s.trim().split("\\s");

            reducerKey.set(Math.round(Float.parseFloat(tokens[1])));
            reducerValue.set(tokens[0]);
            context.write(reducerKey, reducerValue);
        }

    }

    public static class BloomFilterReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        // we receive the name of the stock and the list of all the TimeSeriesData
        // available
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // To be implemented
            // build the unsorted list of timeseries
            List<Text> listIds = new ArrayList<Text>();
            while (values.iterator().hasNext()) {
                listIds.add(values.iterator().next());
            }

            Text outputValue = new Text();

            outputValue.set("rating: " + key.toString() + ", num of ids: " + listIds.size());
            
            context.write(key, outputValue);

            }
        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: BloomFilter <input> <output>");
                System.exit(1);
            }
           
            System.out.println("args[0]: <input>=" + otherArgs[0]);
            System.out.println("args[1]: <output>=" + otherArgs[1]);

            Job job = Job.getInstance(conf, "InMemoryBloomFilter");
            job.setJarByClass(InMemoryBloomFilter.class);

            // set mapper/reducer
            job.setMapperClass(BloomFilterMapper.class);
            job.setReducerClass(BloomFilterReducer.class);

            // define mapper's output key-value
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            // define reducer's output key-value
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            // define I/O
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }


