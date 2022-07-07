package it.unipi.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class Tester {
    public static class TesterMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private final IntWritable reducerKey = new IntWritable();
        private final IntWritable reducerValue = new IntWritable();

        private BloomFilter bf;
        private String bfPath;
        private int bfRating;
        private int total;
        private int falsePositives;

        public void setup(Context context) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            bfRating = conf.getInt("tester.bloomfilterrating", -1);
            bfPath = conf.get("tester.bloomfilterpath");

            Path inputFilePath = new Path(bfPath);
            FileSystem fs = FileSystem.get(conf);

            try (FSDataInputStream fsdis = fs.open(inputFilePath)) {
                //bf = new BloomFilter(600000,3,Hash.MURMUR_HASH);
                bf = new BloomFilter();
                bf.readFields(fsdis);
            } catch (Exception e) {
                throw new IOException("Error while reading bloom filter from file system.", e);
            }
            total = 0;
            falsePositives = 0;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String s = value.toString();
            if (s == null || s.length() == 0)
                return;

            String[] tokens = s.trim().split("\\s");
            
            int movieRating;
            // This try/catch block is here to deal with the 1st line of the input file
            try {
                movieRating = Math.round(Float.parseFloat(tokens[1]));
            }
            catch(Exception e) {
                return;
            }

            boolean isMember = bf.membershipTest(new Key(tokens[0].getBytes()));

            if((movieRating != bfRating) && isMember){
                falsePositives++;
            }

            total++;

        }

        public void cleanup(Context context) throws IOException, InterruptedException {

            reducerKey.set(bfRating);
            reducerValue.set(falsePositives);
            context.write(reducerKey, reducerValue);

            reducerKey.set(0);
            reducerValue.set(total);
            context.write(reducerKey, reducerValue);

            bf = null;

        }

    }

    public static class TesterReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private static String FILTER_OUTPUT_FILE_CONF = "bloomfilter.output.file";

        private final IntWritable outputValue = new IntWritable();

        public void setup(Context context) throws IOException, InterruptedException
        {
            
        }

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

                int sum = 0;
                for(IntWritable value : values) {
                    sum += value.get();
                }

                outputValue.set(sum);
                context.write(key, outputValue);

            }

            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException {

            }

        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 5) {
                System.err.println("Usage: Tester <input file path> <output> <bloomfilter file path> <bloomfilter rating> <linespermap>");
                System.exit(1);
            }
            int linesPerMap = -1, rating = -1;
            try{
                rating = Integer.parseInt(otherArgs[3]);
                linesPerMap = Integer.parseInt(otherArgs[4]);
            }catch (NumberFormatException ex) {
                System.err.println("The parameter(s) linespermap and bloomfilter rating have to be integers");
                System.exit(1);
            }
           
            System.out.println("args[0]: <input file path>=" + otherArgs[0]);
            System.out.println("args[1]: <output>=" + otherArgs[1]);
            System.out.println("args[2]: <bloomfilter file path>=" + otherArgs[2]);
            System.out.println("args[3]: <bloomfilter rating>=" + otherArgs[3]);
            System.out.println("args[4]: <linespermap>=" + otherArgs[4]);

            Job job = Job.getInstance(conf, "Tester");
            job.setJarByClass(Tester.class);

            //job.getConfiguration().set(TesterReducer.FILTER_OUTPUT_FILE_CONF, otherArgs[1] + Path.SEPARATOR + "test" + otherArgs[3]);
            job.getConfiguration().set("tester.bloomfilterpath", otherArgs[2]);
            job.getConfiguration().set("tester.bloomfilterrating", otherArgs[3]);

            // set mapper/reducer
            job.setMapperClass(TesterMapper.class);
            job.setReducerClass(TesterReducer.class);

            // define mapper's output key-value
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);

            // define reducer's output key-value
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);

            // define I/O
            NLineInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + Path.SEPARATOR + "test" + otherArgs[3]));

            job.setInputFormatClass(NLineInputFormat.class);
            // Set number of lines per mapper
            job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
            job.setOutputFormatClass(TextOutputFormat.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
