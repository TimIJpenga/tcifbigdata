package main.java.nl.hu.hadoop.BigShuf;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class LetterCount {

    public static void main(String[] args) throws Exception {

        FileUtils.deleteDirectory(new File("result/lettercount"));

        Job job = new Job();
        job.setJarByClass(LetterCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(LetterCountMapper.class);
//        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(LetterCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}

class LetterCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s");
        for (String s : tokens) {
            s = s.replaceAll("[^\\p{Alpha}]+","");
            s = s.toLowerCase();
            char[] letters = s.toCharArray();
            for (char c : letters) {
                context.write(new Text(c + ""), new IntWritable(1));
            }
        }
    }
}

class LetterCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}