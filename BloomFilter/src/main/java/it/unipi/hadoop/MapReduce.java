package it.unipi.hadoop;

import java.io.IOException;
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
    public static class BloomFilterMapper extends Mapper<LongWritable, Text, Integer, String> {
        // reuse Hadoop's Writable objects
        // private final IntWritable reducerKey = new IntWritable();
        private RatingBloomFilter rbf;


        public void setup(Context context) throws IOException, InterruptedException
        {
            RatingBloomFilterFactory rbff = new RatingBloomFilterFactory();
            rbf = rbff.CreateBloomFilter();
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
            String id = tokens[0];
            float rating = Float.parseFloat(tokens[1]);

            Movie movie = new Movie(id, rating);

            rbf.fillUp(movie);
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            context.write(0, rbf.toString());
        }

    }

    public static class BloomFilterReducer extends Reducer<IntWritable, LongWritable, IntWritable, String> {

        public void reduce(Integer key, String value, Context context)
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


