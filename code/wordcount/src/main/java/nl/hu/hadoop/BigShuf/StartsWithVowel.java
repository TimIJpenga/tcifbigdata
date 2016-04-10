package main.java.nl.hu.hadoop.BigShuf;

import main.java.nl.hu.hadoop.WordCount;
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
import java.util.ArrayList;

import static main.java.nl.hu.hadoop.BigShuf.StartsWithVowel.VOWELS;

public class StartsWithVowel {

    public static final ArrayList<Character> VOWELS = new ArrayList<Character>() {{
        add('a');
        add('e');
        add('i');
        add('o');
        add('u');
    }};

    public static void main(String[] args) throws Exception {

        FileUtils.deleteDirectory(new File("result/starts_with_vowel"));

        Job job = new Job();
        job.setJarByClass(WordCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(StartsWithVowelMapper.class);
        job.setReducerClass(StartsWithVowelReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);

    }
}

class StartsWithVowelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s");

        for (String s : words) {
            s = s.replaceAll("[^\\p{Alpha}]+","");
            s = s.toLowerCase();

            if(!s.equals("") && !s.equals(null)) {
                if(VOWELS.contains(s.charAt(0))){
                    context.write(new Text("ISVOWEL"), new IntWritable(1));
                } else {
                    context.write(new Text("ISNOTVOWEL"), new IntWritable(1));
                }
            }
        }
    }
}

class StartsWithVowelReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }

        context.write(new Text(key), new IntWritable(sum));
    }

}
