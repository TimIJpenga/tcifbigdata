package main.java.nl.hu.hadoop.BigShuf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Analyzer {

    private static Map<String, Integer> letterPatterns;
    private static int highest = 0;
    private static int average = 0;

    public Analyzer() {
        letterPatterns = readLetterPatternResults();
    }

    public static Double predict(String word) throws IOException {


        return 0.0;
    }


    // FEATURE 1

    public static Double letterFrequency(String word) {

        Double divisors = 0.0;
        Double sum = 0.0;
        for (int i = 0; i < word.length(); i++) {

            try {

                Integer letterFrequency = letterPatterns.get(word.charAt(i) + "" + word.charAt(i + 1));
                if (letterFrequency == null) {
                    sum += 0.0;
                } else {
                    sum += (double) letterFrequency / highest * 2;
                }

            } catch (StringIndexOutOfBoundsException e) {}
            divisors++;
        }

        return sum / divisors;
    }

    // FEATURE 2

    public static Double wordSizeAverageLength(String word) {

        int wordSize = word.length();
        int wordSizeAverage = readWordSizeAverageResults();
        int difference = wordSize - wordSizeAverage;
        if (difference < 0) difference = Math.abs(difference);

        Double chance = 1 - (difference * 0.18);
        if (chance > 1 || chance < 0) return 0.0;
        else return chance;
    }


    public static Double calculateLetterAverage(String word, Double chance) {
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

    private static int readWordSizeAverageResults() {

        String fileName = "result/word_size_average/part-r-00000";

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line = br.readLine();
            String[] split = line.split("\\s");

            return Integer.parseInt(split[1]);

        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }
}