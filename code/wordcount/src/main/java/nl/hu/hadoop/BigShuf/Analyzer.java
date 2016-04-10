package main.java.nl.hu.hadoop.BigShuf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Analyzer {

    private static Map<String, Integer> letterPatterns;
    private static Map<String, Integer> letterCount;
    private static int highest = 0;
    private static int average = 0;
    private static int startsWithVowel = 0;
    private static int startsNotWithVowel = 0;

    public Analyzer() {
        letterPatterns = readLetterPatternResults();
        average = readWordSizeAverageResults();
        readStartsWithVowelResults();
        letterCount = readLetterCountResults();
    }

    public static double predict(String word) {

        Double totalChance = 0.0;
        totalChance += letterFrequency(word);
        totalChance += wordSizeAverageLength(word);
        totalChance += chanceStartsWithVowel(word);
        totalChance += calculateBigramChance(word);

        return totalChance / 4;
    }


    // FEATURE 1

    public static double letterFrequency(String word) {

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

        Double chance = sum / divisors;
        if(chance != null) return chance;
        else return 0.0;
    }

    // FEATURE 2

    public static double wordSizeAverageLength(String word) {
        int wordSize = word.length();
        int difference = wordSize - average;
        if (difference < 0) difference = Math.abs(difference);

        Double chance = 1 - (difference * 0.18);
        if (chance > 1 || chance < 0) return 0.0;
        else if (chance != null) return chance;
        else return 0.0;
    }

    // FEATURE 3

    public static double chanceStartsWithVowel(String word) {
        int total = startsWithVowel + startsNotWithVowel;
        try {
            if(Character.isAlphabetic(word.charAt(0))) {
                char c = word.charAt(0);
                Double chance;
                if(c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') {
                    chance = (double) startsWithVowel / total;
                } else chance = (double) startsNotWithVowel / total;

                if(chance != null) return chance;
                else return 0.0;
            }
        } catch (StringIndexOutOfBoundsException e) {}
        return 0.0;
    }

    // FEATURE 4

    public static Double calculateBigramChance(String word) {
        Double chance = 0.0;

        for (int i = 0; i < word.length(); i++) {
            try {

                Double letterSequenceChance;
                Integer letterPatternFrequency = letterPatterns.get(word.charAt(i) + "" + word.charAt(i + 1));
                Integer maxPossible = letterCount.get(word.charAt(i) + "");

                if (letterPatternFrequency == null) letterSequenceChance = 0.0;
                else letterSequenceChance = (double) letterPatternFrequency / maxPossible;

                if (letterSequenceChance < 0.025) {
                    chance = letterFrequency(word);
                } else chance = 1.0;
            } catch (StringIndexOutOfBoundsException e) {}
        }

        if(chance != null) return chance;
        else return 0.0;
    }


    private static Map<String, Integer> readLetterPatternResults() {
        System.out.println("Opening letterpatronen");

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
        System.out.println("Opening wordsizeaverage");

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

    private static void readStartsWithVowelResults() {
        System.out.println("Opening startswithvowel");

        String fileName = "result/starts_with_vowel/part-r-00000";

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;

            while ((line = br.readLine()) != null) {
                String[] split = line.split("\\s");

                if(split[0].equals("ISVOWEL")) startsWithVowel = Integer.parseInt(split[1]);
                else if(split[0].equals("ISNOTVOWEL")) startsNotWithVowel = Integer.parseInt(split[1]);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Integer> readLetterCountResults() {
        System.out.println("Opening lettercount");

        String fileName = "result/lettercount/part-r-00000";

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            Map<String, Integer> letterCount = new HashMap<>();

            String line;

            while ((line = br.readLine()) != null) {
                String[] split = line.split("\\s");

                letterCount.put(split[0], Integer.parseInt(split[1]));
            }
            return letterCount;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}