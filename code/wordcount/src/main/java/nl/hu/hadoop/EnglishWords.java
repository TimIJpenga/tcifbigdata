package main.java.nl.hu.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EnglishWords {

    private static Map<String, Integer> letterPatterns;
    private static int highest = 0;
    private static int average = 0;

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
        System.out.println(calculateLetterAverage("Pluisje", 0.0));
        letterPatterns = readLetterPatternResults();
        System.err.println(ComplexPredict("floepsie"));
        System.err.println(ComplexPredict("train"));
        System.err.println(ComplexPredict("work"));
        System.err.println(ComplexPredict("he"));
    }

    private static Double predict(String word) throws IOException {


        return 0.0;
    }

    private static Double calculateLetterAverage(String word, Double chance) {
        Double difference = word.length() - 5.0;
        if (difference < 0) {
            difference -= difference;
            System.out.println(difference);
        }
        if (difference != 0) {
            chance = chance / difference;
            System.out.println(chance);
        }
        return chance;
    }


    public static Double ComplexPredict(String word) {

        Double divisors = 0.0;
        Double sum = 0.0;
        for (int i = 0; i < word.length(); i++) {

            try {
                sum += calculateChanceofLetterSequence(word.charAt(i), word.charAt(i + 1));
            } catch (StringIndexOutOfBoundsException e) {
//                throw e;
            }
            divisors++;
        }

        return sum / divisors;
    }

    public static Double calculateChanceofLetterSequence(char first, char second) {

        Integer events = letterPatterns.get(first + "" + second);

        if (events == null) {
            return 0.0;
        }
        return (double) events / highest * 2;

    }

    private static Map<String, Integer> readLetterPatternResults() {

        String fileName = "result/letter_patronen/part-r-00000";

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            Map<String, Integer> letterPatterns = new HashMap<>();

            String line;
            int index = 0;
            int total = 0;
            while ((line = br.readLine()) != null) {
                String[] split = line.split("\\s");
                String key = split[0] + split[1];
                Integer value = Integer.parseInt(split[2]);

                letterPatterns.put(key, value);
                total += value;
                if(value > highest) highest = value;
                index++;
            }
            average = total / index;
            return letterPatterns;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
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
