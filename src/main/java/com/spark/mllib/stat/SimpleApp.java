package com.spark.mllib.stat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * README.md
 * 包含spark的行数
 * 包含hadoop的行数打印出来
 * @author Admin
 *
 */
public class SimpleApp {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SimpleApp");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> readRdd = context.textFile("/spark/file/README.md");
		JavaRDD<String>  sparkRdd = readRdd.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String arg0) throws Exception {
				return arg0.contains("spark")?true:false;
			}
		});
		
		JavaRDD<String>  hadoopRdd = readRdd.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String arg0) throws Exception {
				return arg0.contains("hadoop")?true:false;
			}
		});
		
		
		//spark RDD action操作
		long sparkCount = sparkRdd.count();
		long hadoopCount = hadoopRdd.count();
		System.out.println("Read.md文件中包含spark的行数==="+sparkCount);
		System.out.println("Read.md文件中包含hadoop的行数==="+hadoopCount);
	}

}
