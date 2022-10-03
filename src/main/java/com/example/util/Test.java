package com.example.util;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-12-14 16:02
 */
public class Test {
    public static void main(String[] args) {
        boolean A = true;
        boolean B = true;
        boolean C = false;
        System.out.println(A||B&C);
        System.out.println((A||B)&C);
    }
}
