package com.example.java;

//Time & Space Complexity: O(N)
public class Abbreviation {
    public static String getAbbreviate(String input){
        StringBuffer output = new StringBuffer();
        for(String word:input.trim().split("\\s+")){
            System.out.println(word);
            output.append(Character.toUpperCase(word.charAt(0)));
        }

        return output.toString().trim();
    }

    public static void main(String[] args){
        String sentence = "create abbreviation from sentence";
        System.out.println(getAbbreviate(sentence));
    }
}
