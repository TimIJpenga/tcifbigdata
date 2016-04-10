package main.java.nl.hu.hadoop;

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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WordSizeAverage {

    public static JSONArray results;

    public static void main(String[] args) throws Exception {

        FileUtils.deleteDirectory(new File("result/word_size_average"));
        results = new JSONArray();
        JSONObject resultObject = new JSONObject();
        resultObject.put("result", results);

        Job job = new Job();
        job.setJarByClass(WordCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordSizeAverageMapper.class);
        job.setReducerClass(WordSizeAverageReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);

    }
}

class WordSizeAverageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s");

        for (String s : words) {
            s = s.replaceAll("[^\\p{Alpha}]+","");
            s = s.toLowerCase();

            if(!s.equals("") && !s.equals(null)) {
                context.write(new Text("Average"), new IntWritable(s.length()));
            }
        }
    }
}

class WordSizeAverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int index = 0;
        for (IntWritable i : values) {
            sum += i.get();
            index++;
        }

        int average = sum / index;
        context.write(new Text(key), new IntWritable(average));
    }

}
