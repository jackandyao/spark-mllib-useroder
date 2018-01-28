package com.spark.mllib.stat;

import java.math.BigDecimal;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

/**
 * 用户收藏的产品的各个类别的购买率
 * @author Admin
 *
 */
public class MainClassBuyStat {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MainClassBuyStat");
		JavaSparkContext context = new JavaSparkContext(conf);
		SQLContext sqlContext = new HiveContext(context);
		
		DataFrame productDF =sqlContext.sql("select product_id,parent_category_id from dim_productprotectplan "
				+ "where  parent_category_name in('旅游保险','意外保险','人寿保险','健康保险','家财保险')");
		
		DataFrame userFavoriteDF = sqlContext.sql("select UserId,ProductProtectPlanId,CreateTime from dw_evt_favorite");
		
		//获取所有收藏大类的数据
		 DataFrame userProductFavorDF = userFavoriteDF.join(productDF).where("ProductProtectPlanId=product_id");
		 userProductFavorDF.cache();
		 userProductFavorDF.registerTempTable("dw_evt_favorite_cache");
		 DataFrame userProductFavorDFTmp = userProductFavorDF.groupBy("UserId","parent_category_id").count().
				 withColumnRenamed("count", "userFavorCount").withColumnRenamed("parent_category_id", "favorparent_category_id");
		 //把用户收藏表与产品属性表内连接的表，注册成临时表
		 userProductFavorDFTmp.registerTempTable("dw_evt_favorite_tmp");
		 
		 //用户购买的产品信息
		 DataFrame userProductInsure = sqlContext.sql("select t1.passport_id as passport_id,t1.parent_category_id as parent_category_id from dw_evt_insure_pluto as t1"
		 		+ "  join dw_evt_favorite_cache as t2  on t1.passport_id=UserId and t1.product_plan_id=ProductProtectPlanId and t1.create_time>t2.CreateTime");
		 
		 //用户，各主分类的分组后的购买数
		 DataFrame userProductInsureDF =  userProductInsure.groupBy("passport_id","parent_category_id").count().withColumnRenamed("count", "insureCount");
		 userProductInsureDF.registerTempTable("dw_evt_insure_pluto_tmp");
		 
		 
		 
		final long totalSize = sqlContext.sql("select count(0) from dw_evt_favorite_tmp").count();
		System.out.println("收藏的大类产品总共大小===="+totalSize);
		
		sqlContext.udf().register("percent",new UDF1<Long, Double>() {
			@Override
			public Double call(Long insureCount) throws Exception {
				if(insureCount==null){
					return 0.0;
				}
				
				Double r = (insureCount+0.0)/(totalSize+0.0);
				BigDecimal b = new BigDecimal(r);
				Double result = b.setScale(3, BigDecimal.ROUND_UP).doubleValue();
				return result;
			}
		}, DataTypes.DoubleType);
		
		sqlContext.udf().register("convertNull",new UDF1<String, Integer>() {
			@Override
			public Integer call(String parent_category_id) throws Exception {
		          if(StringUtils.isBlank(parent_category_id)){
		        	  return 0;
		          }
				return Integer.valueOf(parent_category_id);
			}
		}, DataTypes.IntegerType);
		
		 
		 //用户id，用户收藏的类别id与用户id，用户购买类别id 左连接
		 DataFrame resultDf = sqlContext.sql("select UserId,favorparent_category_id, convertNull(parent_category_id) as parent_category_id,percent(insureCount) as rate from dw_evt_favorite_tmp left join dw_evt_insure_pluto_tmp"
		 		+ " on UserId=passport_id");
		 
		 DataFrame lastResultDf = resultDf.groupBy("favorparent_category_id","parent_category_id").sum("rate");
		 
		 lastResultDf.show();
		 userProductFavorDF.unpersist();
		 context.stop();
		 
	}
}
