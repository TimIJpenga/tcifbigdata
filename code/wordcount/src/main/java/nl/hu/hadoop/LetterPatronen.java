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

public class LetterPatronen {

    public static JSONArray results;

    public static void main(String[] args) throws Exception {

        FileUtils.deleteDirectory(new File("result/letter_patronen"));
        results = new JSONArray();

        Job job = new Job();
        job.setJarByClass(WordCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(LetterPatronenMapper.class);
//        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(LetterPatronenReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);

        try (FileWriter file = new FileWriter("result/letter_patronen/result.json")) {
            file.write(results.toString());
            System.out.println("Successfully Copied JSON Object to File...");
        }

    }
}

class LetterPatronenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s");

        for (String s : words) {
            s = s.replaceAll("[^\\p{Alpha}]+","");
            s = s.toLowerCase();

            int i = 0;
            char[] word = s.toCharArray();
            for(char c : word) {
//                char before = 'x';
                char after = '!';
//                if(i-1 >= 0) before = word[i-1];
                if(i+1 <= word.length-1) after = word[i+1];

                if(after != '!') {
                    context.write(new Text(c + " " + after), new IntWritable(1));
                }
                i++;
            }

        }
    }
}

class LetterPatronenReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }

        String[] words = key.toString().split("\\s");
        JSONObject result = new JSONObject();
        try {
            result.put("letter", words[0]);
            result.put("nextLetter", words[1]);
            result.put("sum", sum);
        } catch (JSONException e) {

        }

        LetterPatronen.results.put(result);
        context.write(new Text(key), new IntWritable(sum));
    }

}
