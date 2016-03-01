package main.java.nl.hu.hadoop;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
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

public class SortWebsites {

	public static void main(String[] args) throws Exception {

		FileUtils.deleteDirectory(new File("result/sort_websites"));

		Job job = new Job();
		job.setJarByClass(SortWebsites.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(SortWebsitesMapper.class);
		job.setReducerClass(SortWebsitesReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}

class SortWebsitesMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String url = "";
		String[] tokens = value.toString().split("\\s");
        for (String s : tokens) {
            if(s.contains(".com")) url = s;
            else if(url != "") context.write(new Text(s), new Text(url));
        }
    }
}

class SortWebsitesReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String websites = "\t";
        for(Text t : values) {
            websites += t.toString() + " ";
        }
        context.write(key, new Text(websites));
    }
}
