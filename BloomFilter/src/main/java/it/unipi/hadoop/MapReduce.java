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

        private final IntWritable reducerKey = new IntWritable();
        private BloomFilter reducerValue = null;

        private List<BloomFilter> filters;
        private int m, k;

        public void setup(Context context) throws IOException, InterruptedException
        {
            filters = new ArrayList<BloomFilter>(RATING_NUM+1);
            for(int i = 0 ; i <= RATING_NUM ; i++){
                filters.add(i, null);
            }

            Configuration conf = context.getConfiguration();
            m = conf.getInt("bloomfilter.m", 2500000);
            k = conf.getInt("bloomfilter.k", 3);
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

            if(filters.get(rating) != null){
                filters.get(rating).add(new Key(tokens[0].getBytes()));
            }else{
                BloomFilter bf = new BloomFilter(m, k, Hash.MURMUR_HASH);
                bf.add(new Key(tokens[0].getBytes()));
                filters.add(rating, bf);
            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {

            for (int i = 1 ; i <= RATING_NUM ; i++) {
                reducerKey.set(i);
                reducerValue = filters.get(i);
                if(reducerValue != null)
                    context.write(reducerKey, reducerValue);
            }

            reducerValue = null;
            filters = null;

        }

    }

    public static class BloomFilterReducer extends Reducer<IntWritable, BloomFilter, IntWritable, Text> {

        private static String FILTER_OUTPUT_FILE_CONF = "bloomfilter.output.file";

        private BloomFilter result;
        private int m, k;

        public void setup(Context context) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            m = conf.getInt("bloomfilter.m", 2500000);
            k = conf.getInt("bloomfilter.k", 3);

            result = new BloomFilter(m,k,Hash.MURMUR_HASH);
        }

        public void reduce(IntWritable key, Iterable<BloomFilter> values, Context context)
                throws IOException, InterruptedException {

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

                //context.write(key, new Text(result.toString()));

            }

            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException {

            }

        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 5) {
                System.err.println("Usage: BloomFilter <input> <output> <linespermap> <m> <k>");
                System.exit(1);
            }
            int linesPerMap = -1, m = -1, k = -1;
            try{
                linesPerMap = Integer.parseInt(otherArgs[2]);
                m = Integer.parseInt(otherArgs[3]);
                k = Integer.parseInt(otherArgs[4]);
            }catch (NumberFormatException ex) {
                System.err.println("The parameters linespermap, m, and k have to be integers");
                System.exit(1);
            }
           
            System.out.println("args[0]: <input>=" + otherArgs[0]);
            System.out.println("args[1]: <output>=" + otherArgs[1]);
            System.out.println("args[2]: <linespermap>=" + otherArgs[2]);
            System.out.println("args[3]: <m>=" + otherArgs[3]);
            System.out.println("args[4]: <k>=" + otherArgs[4]);

            Job job = Job.getInstance(conf, "MapReduce");
            job.setJarByClass(MapReduce.class);

            job.getConfiguration().set(BloomFilterReducer.FILTER_OUTPUT_FILE_CONF, otherArgs[1] + Path.SEPARATOR + "filters");
            job.getConfiguration().setInt("bloomfilter.m", m);
            job.getConfiguration().setInt("bloomfilter.k", k);

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
            NLineInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            job.setInputFormatClass(NLineInputFormat.class);
            // Set number of lines per mapper
            job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
            job.setOutputFormatClass(TextOutputFormat.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
