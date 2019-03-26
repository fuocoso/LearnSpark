package com.learn.javaDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WebPVUvSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().
                setAppName("Java Spark WordCount")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("data/2015082818");






    }
}
