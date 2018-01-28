package com.spark.mllib.stat;//package com.xtwy.pro_sp;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//public class MysqlConnecter {
//	
//	public static void main(String[] args) {
//		Map<String, String> options = new HashMap<>();
//		options.put("url", "jdbc:mysql://192.168.1.102:3306/test");
//		options.put("dbtable", "t_user");
//		options.put("user", "root");
//		options.put("password", "123456");
//		options.put("driver", "com.mysql.jdbc.Driver");
//        SparkSession spark = SparkSession.builder().master("spark://hmaster:7077").
//        		appName("MysqlConnecter").
//        		enableHiveSupport().
//        		getOrCreate();
////        Dataset<Row> jdbcDF =   spark.sql("select * from t_user");
//        
//        Dataset<Row> favorite = spark.read().csv("/spark/file/dw_evt_favorite.csv");
//		Dataset<Row> jdbcDF = spark.read().format("jdbc"). options(options).load();
//		jdbcDF.write().saveAsTable("t_user");
//		favorite.write().saveAsTable("dw_evt_favorite");
//		
//		jdbcDF.show();
//		spark.stop();
//		
//		
//	}
//
//}
