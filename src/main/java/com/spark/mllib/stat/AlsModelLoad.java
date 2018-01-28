package com.spark.mllib.stat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.bson.Document;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.xtwy.pro_sp.stat.model.UserProduct;

public class AlsModelLoad {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("AlsModelLoad")
				  .set("spark.mongodb.output.uri", "mongodb://192.168.10.141:27017/test.myCollection");;
		JavaSparkContext context = new JavaSparkContext(conf);
		SQLContext sqlContext = new HiveContext(context);
		DataFrame productDF =sqlContext.sql("select id,name,parent_category_id from dim_productprotectplan "
				+ "where  parent_category_name in('旅游保险','意外保险','人寿保险','健康保险','家财保险')");
		productDF.cache();
		Row[] products = productDF.collect();
		Map<Integer,String> productMap = new HashMap<Integer,String>();
		for(Row product:products){
			productMap.put(product.getInt(0), product.getString(1));
		}
		
	   DataFrame userFavoriteDF = sqlContext.sql("select UserId as user,ProductProtectPlanId as product,0.1 as rating,CreateTime from dw_evt_favorite");
		//获取所有收藏大类的数据
		 DataFrame userProductFavorDF = userFavoriteDF.join(productDF,productDF.col("id").equalTo(userFavoriteDF.col("product")),"inner").select("user","product","rating");

		MatrixFactorizationModel model = MatrixFactorizationModel.load(context.sc(),"/spark/alsmodel/");
		
		Row[] users = userProductFavorDF.limit(10).collect();
		List<Document> docs = new ArrayList<Document>();
		 for(Row user : users){
			 Rating[]  results = model.recommendProducts(user.getInt(0), 5);
			 for(Rating r : results){
				 System.out.println("用户id为===340959推荐的产品id为==="+r.product()+"产品名称为："+productMap.get(r.product())+"   推荐的成功率==="+r.rating());
				 UserProduct up = new UserProduct();
				 up.setProductId(r.product());
				 up.setRating(r.rating());
				 up.setUserId(r.user());
				 docs.add(Document.parse(JSONObject.toJSONString(up)));
			 }
		 }
		 
       Map<String, String> writeOverrides = new HashMap<String, String>();
       writeOverrides.put("collection", "user_product_rating");
       writeOverrides.put("writeConcern.w", "majority");
       WriteConfig writeConfig = WriteConfig.create(context).withOptions(writeOverrides);
       
       JavaRDD<Document> documents =context.parallelize(docs);
	    MongoSpark.save(documents , writeConfig);
		 productDF.unpersist();
		 context.stop();
	}

}
