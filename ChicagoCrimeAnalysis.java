package com.mapreduce.cca;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChicagoCrimeAnalysis {

    // Job 1: Crime Types Analysis
    public static class CrimeTypeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text crimeType = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().contains("Case Number")) {
                return;
            }

            try {
                String[] fields = value.toString().split(",");
                String primaryType = fields[0].trim();
                
                if (!primaryType.isEmpty()) {
                    crimeType.set("Primary Type: " + primaryType);
                    context.write(crimeType, one);
                }
            } catch (Exception e) {
                context.getCounter("CrimeTypeMapper", "Malformed Records").increment(1);
            }
        }
    }

    public static class CrimeTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Job 2: Community Area Analysis
    public static class CommunityAreaMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private IntWritable communityArea = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().contains("Case Number")) {
                return;
            }

            try {
                String[] fields = value.toString().split(",");
                String communityAreaStr = fields[2].trim();
                
                if (!communityAreaStr.isEmpty()) {
                    int area = Integer.parseInt(communityAreaStr);
                    if (area >= 1 && area <= 77) {
                        communityArea.set(area);
                        context.write(communityArea, one);
                    }
                }
            } catch (Exception e) {
                context.getCounter("CommunityAreaMapper", "Malformed Records").increment(1);
            }
        }
    }

    public static class CommunityAreaReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Job 3: Temporal Crime Trends (Month/Year)
    public static class TemporalMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text timeUnit = new Text();
        private static final IntWritable one = new IntWritable(1);
        private SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 2 && !fields[1].isEmpty()) { // Ensure Date column is valid
                try {
                    // Parse the date using the provided format
                    Date date = inputFormat.parse(fields[1].trim());
                    
                    // Create a new format to output the "month/year"
                    SimpleDateFormat outputFormat = new SimpleDateFormat("MM/yyyy");
                    String monthYear = outputFormat.format(date);
                    
                    timeUnit.set(monthYear);
                    context.write(timeUnit, one);
                } catch (Exception e) {
                    context.getCounter("TemporalMapper", "Malformed Date").increment(1);
                }
            }
        }
    }

    public static class TemporalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: ChicagoCrimeAnalysis <input path> <output path 1> <output path 2> <output path 3>");
            System.exit(-1);
        }

        // Job 1: Crime Types Analysis
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Chicago Crime Types Analysis");
        job1.setJarByClass(ChicagoCrimeAnalysis.class);
        job1.setMapperClass(CrimeTypeMapper.class);
        job1.setReducerClass(CrimeTypeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // Job 2: Community Area Analysis
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Chicago Community Area Analysis");
        job2.setJarByClass(ChicagoCrimeAnalysis.class);
        job2.setMapperClass(CommunityAreaMapper.class);
        job2.setReducerClass(CommunityAreaReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

        // Job 3: Temporal Analysis (Month/Year)
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Chicago Temporal Crime Analysis");
        job3.setJarByClass(ChicagoCrimeAnalysis.class);
        job3.setMapperClass(TemporalMapper.class);
        job3.setReducerClass(TemporalReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
