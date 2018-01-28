package com.spark.mllib.sql;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class ImportMysql {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("ImportMysql");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		SQLContext sqlContext = new HiveContext(context);
		
//		Properties properties =new Properties();
//		properties.put("driver", "com.mysql.jdbc.Driver");
//		properties.put("user", "root");
//		properties.put("password", "huize123");
//		DataFrame  userDataFrame = sqlContext.read().format("jdbc").
//				jdbc("jdbc:mysql://192.168.10.41:3306/test", "t_word_wc", properties );
//		
//		userDataFrame.write().saveAsTable("t_word_wc");
		
		DataFrame userDataFrame = sqlContext.sql("select * from t_word_wc");
		userDataFrame.show();
		context.stop();
	}

}
