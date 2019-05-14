package ru.beeline.bdrit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class App {
    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("Hello Spark");
        sparkConf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD rdd = context.textFile("/home/alexbulatov/data/pagecounts");

        rdd.take(10).forEach(x -> System.out.println(x));
        context.close();
    }
}
