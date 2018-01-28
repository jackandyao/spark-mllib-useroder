package com.spark.mllib.mongodb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

public class ExchangeMongodb {
	
	public static void main(String[] args) {
		SparkConf sc = new SparkConf()
        .setMaster("local")
        .setAppName("MongoSparkConnectorTour")
        .set("spark.mongodb.input.uri", "mongodb://192.168.10.141:27017/test.myCollection")
        .set("spark.mongodb.output.uri", "mongodb://192.168.10.141:27017/test.myCollection");

      JavaSparkContext jsc = new JavaSparkContext(sc); // Create a Java Spark Context
       JavaRDD<Document> documents = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
            (new Function<Integer, Document>() {
        public Document call(final Integer i) throws Exception {
            return Document.parse("{test: " + i + "}");
        }
      });
       
//       MongoSpark.save(documents);
       
//       Map<String, String> writeOverrides = new HashMap<String, String>();
//       writeOverrides.put("collection", "spark");
//       writeOverrides.put("writeConcern.w", "majority");
//       WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
//       
//       MongoSpark.save(documents, writeConfig);
//    
       
       JavaRDD<Document> rdd = MongoSpark.load(jsc);
       System.out.println(rdd.count());
       System.out.println(rdd.first().toJson());
       
//       Map<String, String> readOverrides = new HashMap<String, String>();
//       readOverrides.put("collection", "spark");
//       readOverrides.put("readPreference.name", "secondaryPreferred");
//       ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
//       JavaRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);
//       System.out.println(customRdd.count());
//       System.out.println(customRdd.first().toJson());
       
    jsc.stop();
    
	}

}
