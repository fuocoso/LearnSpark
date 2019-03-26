package com.learn.javaDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.datanucleus.store.rdbms.query.AbstractRDBMSQueryResult;
import scala.Tuple1;
import scala.Tuple2;

import java.sql.SQLException;
import java.util.*;

public class Test {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Test")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> strRdd = jsc.textFile("data/test.txt");
        System.out.println(strRdd.count());
        JavaPairRDD<String, Tuple2<String, Integer>> pairRDD = strRdd.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(String s) throws Exception {
                String[] split = s.split(",");
                String course = split[0];
                String name = split[1];
                Integer num = Integer.valueOf(split[2]);
                return new Tuple2<String, Tuple2<String, Integer>>(split[0], new Tuple2<String, Integer>(name, num));
            }
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = pairRDD.groupByKey();



    }
}
