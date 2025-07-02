package com.example.java;

import java.util.Arrays;

public class ReverseArray {

    public static int[] getReverseArray(int[] input){
        int start = 0;
        int end = input.length;
       while(start < input.length/2){
           input[start] = input[start] + input[end-1];
           input[end-1] = input[start] - input[end-1];
           input[start] = input[start] - input[end-1];
           start++;
           end--;
       }
        return input;
    }

    public static void main(String[] km){
        int[] data = {1, 2, 5, 7, 11, 12, 3, 6};
        System.out.println(Arrays.toString(getReverseArray(data)));
    }
}
