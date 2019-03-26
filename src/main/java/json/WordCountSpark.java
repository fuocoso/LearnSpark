package com.learn.javaDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCountSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().
                setAppName("Java Spark WordCount")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("data/wc.txt");

        /**
         * public interface FlatMapFunction<T, R> extends Serializable {
         *   Iterator<R> call(T var1) throws Exception;
         * }
         * T作为函数的输入类型  也就是第一个参数为输入的类型
         * R作为返回类型 第二个为返回的类型
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
         @Override
         public Iterator<String> call(String s) throws Exception {
             return Arrays.asList(s.split(" ")).iterator();
         }
     });



        /**
         * public interface Function<T1, R> extends Serializable {
         *  R call(T1 var1) throws Exception;
         * }
         * 第一个参数作为输入参数 第二个作为返回类型
         */
     JavaRDD<String> filter = words.filter(new Function<String, Boolean>() {
         @Override
         public Boolean call(String Lines) throws Exception {
             return lines != null;
         }
     });


        /**
         * public interface PairFunction<T, K, V> extends Serializable {
         *   Tuple2<K, V> call(T var1) throws Exception;
         * }
         * Tuple2<K, V>作为返回类型  T作为输入类型
         * 也就是输入一个字串，返回一对元组
         */

     JavaPairRDD<String,Integer> pairs = filter.mapToPair(new PairFunction<String, String,Integer>() {

         @Override
         public Tuple2<String, Integer> call(String word) throws Exception {
             return new Tuple2<String,Integer>(word,1);
         }
     });

        /**
         *  public interface Function2<T1, T2, R> extends Serializable {
         * R call(T1 var1, T2 var2) throws Exception;
         * }
         * T1, T2,作为输入参数，R作为返回类型
         * 输入两个整形 返回一个整形
         */
     JavaPairRDD<String,Integer> wc = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
         @Override
         public Integer call(Integer i1, Integer i2) throws Exception {
             return i1+i2;
         }
     });

     wc.foreach(new VoidFunction<Tuple2<String, Integer>>() {
         @Override
         public void call(Tuple2<String, Integer> wordcount) throws Exception {
             System.out.println(wordcount._1+"->"+wordcount._2);
         }
     });
        List<Tuple2<String, Integer>> res = wc.collect();
        for (Tuple2<?,?> tuple : res){
            System.out.println(tuple._1+"=>"+tuple._2);
        }
        sc.stop();

    }
}
