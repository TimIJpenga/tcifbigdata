package main.java.nl.hu.hadoop.BigShuf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EnglishWords {

    public static void main(String[] args) throws Exception {

//        FileUtils.deleteDirectory(new File("result/english_words"));
//
//        Job job = new Job();
//        job.setJarByClass(WordCount.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        job.setMapperClass(EnglishWordsMapper.class);
////        job.setCombinerClass(WordCountReducer.class);
//        job.setReducerClass(EnglishWordsReducer.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);

//        job.waitForCompletion(true);

//        System.out.println(predict("Pluisje"));
        Analyzer analyzer = new Analyzer();
//        System.out.println(analyzer.calculateLetterAverage("Pluisje", 0.0));
//        System.err.println(analyzer.letterFrequency("floepsie"));
//        System.err.println(analyzer.letterFrequency("train"));
//        System.err.println(analyzer.letterFrequency("work"));
//        System.err.println(analyzer.letterFrequency("he"));
        System.err.println(analyzer.wordSizeAverageLength("a"));
        System.err.println(analyzer.wordSizeAverageLength("jfdd"));
        System.err.println(analyzer.wordSizeAverageLength("jfddgdf"));
        System.err.println(analyzer.wordSizeAverageLength("jfddgdfgfdhfd"));
    }

}

class EnglishWordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s");

        for (String s : words) {
            s = s.replaceAll("[^\\p{Alpha}]+","");
            s = s.toLowerCase();

//            int i = 0;
//            char[] word = s.toCharArray();
            context.write(new Text(s), new IntWritable(1));

//            for(char c : word) {
////                char before = 'x';
//                char after = '!';
////                if(i-1 >= 0) before = word[i-1];
////                if(i+1 <= word.length-1) after = word[i+1];
//
//                if(after != '!') {
//                    context.write(new Text(c + " " + after), new IntWritable(1));
//                }
//                i++;
//                System.err.println(word);
//            }

        }
    }

}

class EnglishWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }

        context.write(new Text(key), new IntWritable(sum));
    }

}
