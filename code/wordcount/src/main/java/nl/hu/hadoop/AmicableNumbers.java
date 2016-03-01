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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class AmicableNumbers {

    public static void main(String[] args) throws Exception {

        final int NUMBER_AMOUNT = 100000;
        final int NUMBER_SIZE = 100000;

        PrintWriter out = new PrintWriter(new File("data/mapreduce/random_numbers.txt"));
        Random rand = new Random();
        int number, count=0, countTwo=0;
        while(count<NUMBER_AMOUNT)
        {
            while(countTwo<15)
            {
                number=rand.nextInt(NUMBER_SIZE)+1;
                out.print(number + " ");
                count++;
                countTwo++;
            }
            countTwo = 0;
        }
        out.close();

        FileUtils.deleteDirectory(new File("result/amicable_numbers"));

        Job job = new Job();
        job.setJarByClass(AmicableNumbers.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AmicableNumbersMapper.class);
//        job.setCombinerClass(AmicableNumbersReducer.class);
        job.setReducerClass(AmicableNumbersReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);

    }

}

class AmicableNumbersMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArray = value.toString().split("\\s");

        int[] numbers = new int[strArray.length];
        for(int i = 0; i < strArray.length; i++) {
            numbers[i] = Integer.parseInt(strArray[i]);
        }
        numbers = removeDuplicates(numbers);

        for (int number : numbers) {
//            int number = Integer.parseInt(s);
            int amicableValue = getAmicableValue(number);

            context.write(new IntWritable(number), new IntWritable(amicableValue));
        }
    }

    public static int[] removeDuplicates(int[] arr) {
        Set<Integer> alreadyPresent = new HashSet<>();
        int[] whitelist = new int[arr.length];
        int i = 0;

        for (int element : arr) {
            if (alreadyPresent.add(element)) {
                whitelist[i++] = element;
            }
        }

        return Arrays.copyOf(whitelist, i);
    }

    private int getAmicableValue(int input) {
        int sum = 1;
        int sqrt = (int)Math.sqrt(input);
        for (int i = 2; i <= 1 + sqrt; i++)
            if (input % i == 0) sum = sum + i + input / i;
        return sum;
    }
}

class AmicableNumbersReducer extends Reducer<IntWritable, IntWritable, Text, Text> {
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for(IntWritable i : values) {
            if(isAmicablePair(key.get(), i.get()))
                context.write(new Text("Deze getallen zijn bevriend met elkaar"), new Text(key.get() + " <-> " + i.get()));
        }
    }

    public boolean isAmicablePair(int num1, int num2) {
        if(num1 == num2) return false;

        int sum1=0,sum2=0;
        for(int i=1;i<=num1/2;i++)
        {
            if(num1%i==0)
            {
                sum1+=i;
            }
        }

        for(int i=1;i<=num2/2;i++)
        {
            if(num2%i==0)
            {
                sum2+=i;
            }
        }

        if(sum1==num2 && sum2==num1)
            return true;
        else
            return false;
    }

}