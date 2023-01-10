package com.bar.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class RadixSorting {

    public static void main(String[] args) {

        List<LinkedList<Integer>> digitsLinkedNumbers = new ArrayList<>();
        initializeList(digitsLinkedNumbers);
        LinkedList<Integer> numbers = new LinkedList<>(Arrays.asList(56, 77, 23, 7, 356, 99, 889));
        digitsLinkedNumbers.add(0, numbers);
        for (int i = 0; i < 3; i++) {

            digitsLinkedNumbers = sort(digitsLinkedNumbers, i);
        }
        printResult(digitsLinkedNumbers);
    }

    public static List<LinkedList<Integer>> sort(List<LinkedList<Integer>> digitsLinkedNumbers, int digitLoc) {

        List<LinkedList<Integer>> newDigitsLinkedNumbers = new ArrayList<>();
        initializeList(newDigitsLinkedNumbers);
        for (LinkedList<Integer> linkedNumbers : digitsLinkedNumbers) {

            for (Integer i : linkedNumbers) {
                int digit = getDigit(i, digitLoc);
                if (digit == -1)
                    digit = 0;
                LinkedList<Integer> newLinkedNumbers = newDigitsLinkedNumbers.get(digit);
                newLinkedNumbers.addLast(i);
                newDigitsLinkedNumbers.set(digit, newLinkedNumbers);
            }
        }
        return newDigitsLinkedNumbers;
    }

    private static int getDigit(int number, int location) {

        int i = 0;
        while (number > 0) {
            if (i == location)
                return number % 10;
            else
                number = number / 10;
            i++;
        }
        return -1;
    }

    private static void initializeList(List<LinkedList<Integer>> newDigitsLinkedNumbers) {

        for (int i = 0; i < 10; i++) {

            LinkedList<Integer> numbers = new LinkedList<>();
            newDigitsLinkedNumbers.add(i, numbers);
        }
    }

    private static void printResult(List<LinkedList<Integer>> digitsLinkedNumbers) {

        for (LinkedList<Integer> numbers : digitsLinkedNumbers) {

            for (Integer i : numbers) {

                System.out.println(":" + i + ":");
            }
        }
    }
}
