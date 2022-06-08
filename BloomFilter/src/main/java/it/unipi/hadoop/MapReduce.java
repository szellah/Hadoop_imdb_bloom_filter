package it.unipi.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

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


public class MapReduce {
    public static class BloomFilterMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
        // reuse Hadoop's Writable objects
        private final IntWritable reducerKey = new IntWritable();
        private final LongWritable reducerValue = new LongWritable();

        private Map<Integer, Long> associativeArray;

        public void setup(Context context) throws IOException, InterruptedException
        {
            this.associativeArray = new HashMap<Integer, Long>();
            associativeArray.put(0, 0l);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String s = value.toString();
            if (s == null || s.length() == 0)
                return;

            String[] tokens = s.trim().split("\\s");

            //reducerKey.set(Math.round(Float.parseFloat(tokens[1])));
            //reducerValue.set(tokens[0]);
            //context.write(reducerKey, reducerValue);
            int rating = Math.round(Float.parseFloat(tokens[1]));
            Long count = associativeArray.get(rating);
            if( count == null ) {
                count = 0l;
            }
            associativeArray.put(rating, count + 1);

            long total = associativeArray.get(0);
            associativeArray.put(0, total+1);
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            for(Integer rating : associativeArray.keySet()) {
                reducerKey.set(rating);
                reducerValue.set(associativeArray.get(rating));
                context.write(reducerKey, reducerValue);
            }
        }

    }

    public static class BloomFilterReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

        public void reduce(IntWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            
            //List<String> listIds = new ArrayList<String>();
            /*
            long sum = 0;
            for ( Text id : values ) {
                //listIds.add( id.toString() );
                sum = sum + 1;
            }
            
            LongWritable outputValue = new LongWritable();

            outputValue.set(sum);
            */
            context.write(key, value);

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

            // set mapper/reducer
            job.setMapperClass(BloomFilterMapper.class);
            job.setReducerClass(BloomFilterReducer.class);

            // define mapper's output key-value
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(LongWritable.class);

            // define reducer's output key-value
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(LongWritable.class);

            // define I/O
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }


