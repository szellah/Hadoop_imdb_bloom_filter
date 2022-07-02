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
import org.apache.hadoop.fs.FileSystem;

public class MapReduce {
    public static class BloomFilterMapper extends Mapper<LongWritable, Text, IntWritable, BloomFilter> {
        
        private static final int RATING_NUM = 10;

        // reuse Hadoop's Writable objects
        private final IntWritable reducerKey = new IntWritable();
        private BloomFilter reducerValue = null;

        private List<BloomFilter> filters;

        //private List<List<Text>> associativeArray;

        public void setup(Context context) throws IOException, InterruptedException
        {
            /*
            associativeArray = new ArrayList<List<Text>>(RATING_NUM + 1);
            associativeArray.add(null);
            for( int i = 0 ; i < RATING_NUM ; i++ ) {
                associativeArray.add(new LinkedList<Text>());
            }
            */
            filters = new ArrayList<BloomFilter>(RATING_NUM+1);
            for(int i = 0 ; i <= RATING_NUM ; i++){
                filters.add(i, null);
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String s = value.toString();
            if (s == null || s.length() == 0)
                return;

            String[] tokens = s.trim().split("\\s");
            
            int rating;
            // This try/catch block is here to deal with the 1st line of the input file
            try {
                rating = Math.round(Float.parseFloat(tokens[1]));
            }
            catch(Exception e) {
                return;
            }

            /*
            List<Text> ids = associativeArray.get(rating);
            ids.add(new Text(tokens[0]));
            */

            if(filters.get(rating) != null){
                filters.get(rating).add(new Key(tokens[0].getBytes()));
            }else{
                filters.add(rating, new BloomFilter(1000, 3, Hash.MURMUR_HASH));
            }
            
            /*
            reducerKey.set(rating);
            reducerValue.set(tokens[0]);
            context.write(reducerKey, reducerValue);
            */
            
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            /*
            for( int i = 1 ; i <= RATING_NUM ; i++ ) {
                reducerKey.set(i);
                reducerValue.set( (Text[]) associativeArray.get(i).toArray() );
                context.write(reducerKey, reducerValue);
            }
            */

            for (int i = 1 ; i <= RATING_NUM ; i++) {
                reducerKey.set(i);
                reducerValue = filters.get(i);
                if(reducerValue != null)
                    context.write(reducerKey, reducerValue);
            }
        }

    }

    public static class BloomFilterReducer extends Reducer<IntWritable, BloomFilter, IntWritable, Text> {

        private static String FILTER_OUTPUT_FILE_CONF = "bloomfilter.output.file";

        private BloomFilter result = new BloomFilter(1000,3,Hash.MURMUR_HASH);

        public void reduce(IntWritable key, Iterable<BloomFilter> values, Context context)
                throws IOException, InterruptedException {
                /*
                int sum = 0;

                IntWritable outputValue = new IntWritable(); // reuse object

                for (Text txtData : values) {
                    sum++;
                }

                outputValue.set(sum);
                context.write(key, outputValue);
                */

                for (BloomFilter bf : values) {
                    result.or(bf);
                }

                Path outputFilePath = new Path(context.getConfiguration().get(FILTER_OUTPUT_FILE_CONF)+Path.SEPARATOR+Integer.toString(key.get()));
                FileSystem fs = FileSystem.get(context.getConfiguration());

                try (FSDataOutputStream fsdos = fs.create(outputFilePath)) {
                    result.write(fsdos);

                } catch (Exception e) {
                    throw new IOException("Error while writing bloom filter to file system.", e);
                }

            }

            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException {

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

            Job job = Job.getInstance(conf, "MapReduce");
            job.setJarByClass(MapReduce.class);

            job.getConfiguration().set(BloomFilterReducer.FILTER_OUTPUT_FILE_CONF, otherArgs[1] + Path.SEPARATOR + "filter");

            // set mapper/reducer
            job.setMapperClass(BloomFilterMapper.class);
            job.setReducerClass(BloomFilterReducer.class);

            // define mapper's output key-value
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(BloomFilter.class);

            // define reducer's output key-value
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            // define I/O
            //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            NLineInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            //job.setInputFormatClass(TextInputFormat.class);
            job.setInputFormatClass(NLineInputFormat.class);
            // Set number of lines per mapper
            job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100000);
            job.setOutputFormatClass(TextOutputFormat.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
    
