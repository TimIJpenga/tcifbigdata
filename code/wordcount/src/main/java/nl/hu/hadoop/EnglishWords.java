package main.java.nl.hu.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

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
        readLetterPatternResults();
    }

    private static Double predict(String word) throws IOException {
        Stream letterPatronen = Files.lines(Paths.get("result/letter_patronen/part-r-00000"), StandardCharsets.UTF_8);
        System.err.println("DIT ZIJN DE PATRONEN:\n" + letterPatronen.findFirst());

        return 0.0;
    }

    private static void readLetterPatternResults() {

        String fileName = "result/letter_patronen/part-r-00000";

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {

            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
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

    private Double predict(String word) throws IOException {
        Stream letterPatronen = Files.lines(Paths.get("result/letter_patronen/part-r-00000"), StandardCharsets.UTF_8);
        System.err.println("DIT ZIJN DE PATRONEN:\n" + letterPatronen);

        return 0.0;
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
