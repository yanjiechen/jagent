package com.sohu.cloudno.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class StackTrace {

    /**
     * Get the stack trace of the exception.
     * 
     * @param e
     *            The exception instance.
     * @return The full stack trace of the exception.
     */
    public static String getExceptionTrace(Throwable e) {
        if (e != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return sw.toString();
        }
        return "No Exception";
    }

    public static void main(String[] args) {
        try {
            print(args);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("************");
            System.out.println(getExceptionTrace(e));
        }
    }

    private static void print(String[] args) {
        System.out.println(args[0]);
    }
}