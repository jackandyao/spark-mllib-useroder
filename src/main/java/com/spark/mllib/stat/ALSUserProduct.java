package com.spark.mllib.stat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 用户收藏的产品的各个类别的购买率
 * @author Admin
 *
 */
public class ALSUserProduct {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("RandomForestMainClassBuyStat");
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
		 userProductFavorDF.cache();
		 userProductFavorDF.registerTempTable("dw_evt_favorite_cache");
		 
		 //用户购买的产品信息
		 DataFrame userProductInsure = sqlContext.sql("select passport_id as user,product_plan_id as product ,0.01 as rating from dw_evt_insure_pluto");
		 
		 userProductInsure.registerTempTable("dw_evt_insure_pluto_tmp");
		 
		 final Map<Integer,List<Integer>> userProductInsureMap = new HashMap<Integer,List<Integer>>();
		 
		 userProductInsure.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
			@Override
			public void call(Iterator<Row> t) throws Exception {
				while(t.hasNext()){
					Row r = t.next();
					if(userProductInsureMap.containsKey(r.getInt(0))){
						List<Integer> products = userProductInsureMap.get(r.getInt(0));
						products.add(r.getInt(1));
						userProductInsureMap.put(r.getInt(0),products);
					}else{
						List<Integer> products = new ArrayList<Integer>();
						products.add(r.getInt(1));
						userProductInsureMap.put(r.getInt(0),products);
					}
				
				}
				
			}
		});
		 DataFrame unionDataFrame = userProductFavorDF.unionAll(userProductInsure);
		 
		 JavaRDD<Rating> ratings = unionDataFrame.toJavaRDD().map(new Function<Row, Rating>() {
			@Override
			public Rating call(Row v1) throws Exception {
				if(v1.get(0)==null || StringUtils.isBlank(v1.get(0).toString())){
					return null;
				}
				if(v1.get(1)==null || StringUtils.isBlank(v1.get(1).toString())){
					return null;
				}
				
				Rating r = new Rating(Integer.valueOf(v1.get(0).toString()), Integer.valueOf(v1.get(1).toString()), Double.valueOf(v1.get(2).toString()));
				return r;
			}
		}).filter(new Function<Rating, Boolean>() {
			@Override
			public Boolean call(Rating v1) throws Exception {
				if(v1==null){
					return false;
				}
				return true;
			}
		});
		 
		 MatrixFactorizationModel model =  ALS.train(ratings.rdd(), 10, 10);
		 
		 Row[] users = userProductFavorDF.limit(10).collect();
		 for(Row user : users){
			 Rating[]  results = model.recommendProducts(user.getInt(0), 5);
			 for(Rating r : results){
				 if(userProductInsureMap.containsKey(r)){
					 if(!userProductInsureMap.get(r).contains(r.product())){
						 System.out.println("用户id为===340959推荐的产品id为==="+r.product()+"产品名称为："+productMap.get(r.product())+"   推荐的成功率==="+r.rating());
					 }
				 }
			 }
		 }
		 userProductFavorDF.unpersist();
		 context.stop();
		 
	}
}
