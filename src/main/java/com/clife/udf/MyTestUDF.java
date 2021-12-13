package com.clife.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MyTestUDF extends UDF {
    public static A a = new A("aid","aage");

    public String evaluate (final String s) {

        if (s == null) {
            return null;
        }
        if (s.equalsIgnoreCase("b")){
            a.age = "eee";
        }
        System.out.println(s + "||a.hash " + a.hashCode() + "||a.string" + a.toString());
        return s + "||a.hash " + a.hashCode() + "||a.string" + a.toString();
//        return s.toLowerCase();
    }
    static class A{
        String id ;
        String age;

        public A(String id, String age) {
            this.id = id;
            this.age = age;
        }

        @Override
        public String toString() {
            return "id : " + id + " --- age : " + age;
        }
    }

    public static void main(String[] args) {
        new MyTestUDF().evaluate("a");
        new MyTestUDF().evaluate("b");
        new MyTestUDF().evaluate("c");
    }
}
