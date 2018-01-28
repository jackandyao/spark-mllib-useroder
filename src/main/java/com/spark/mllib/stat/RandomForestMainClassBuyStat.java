package com.spark.mllib.stat;

import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.xtwy.pro_sp.sql.MysqlUtil;

/**
 * 用户收藏的产品的各个类别的购买率
 * @author Admin
 *
 */
public class RandomForestMainClassBuyStat {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("RandomForestMainClassBuyStat");
		JavaSparkContext context = new JavaSparkContext(conf);
		SQLContext sqlContext = new HiveContext(context);
		
		DataFrame productDF =sqlContext.sql("select id,parent_category_id from dim_productprotectplan "
				+ "where  parent_category_name in('旅游保险','意外保险','人寿保险','健康保险','家财保险')");
		
		DataFrame userFavoriteDF = sqlContext.sql("select UserId,ProductProtectPlanId,CreateTime from dw_evt_favorite");
		
		//获取所有收藏大类的数据
		 DataFrame userProductFavorDF = userFavoriteDF.join(productDF,productDF.col("id").equalTo(userFavoriteDF.col("ProductProtectPlanId")),"inner");
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
		 
		 
		 
		final long totalSize = sqlContext.sql("select sum(userFavorCount) from dw_evt_favorite_tmp").collect()[0].getLong(0);
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
		 
		 VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(
				 new String[]{"UserId","favorparent_category_id","parent_category_id"}).setOutputCol("features");
		 DataFrame vectorAssemblerDF = vectorAssembler.transform(resultDf);
		 
		 VectorIndexerModel featureIndexer = new VectorIndexer()
		  .setInputCol("features")
		  .setOutputCol("indexedFeatures")
		  .setMaxCategories(4)
		  .fit(vectorAssemblerDF);

		// Split the data into training and test sets (30% held out for testing)
		DataFrame[] splits = vectorAssemblerDF.randomSplit(new double[] {0.7, 0.3});
		DataFrame trainingData = splits[0];
		DataFrame testData = splits[1];

		// Train a RandomForest model.
		RandomForestRegressor rf = new RandomForestRegressor()
		  .setLabelCol("rate")
		  .setFeaturesCol("indexedFeatures");
		
		DecisionTreeRegressor dt = new DecisionTreeRegressor()
		  .setLabelCol("rate")
		  .setFeaturesCol("indexedFeatures");
		// Chain indexer and forest in a Pipeline
		Pipeline pipeline = new Pipeline()
		  .setStages(new PipelineStage[] {featureIndexer, rf});

		PipelineModel  model = pipeline.fit(trainingData);
		
		
		DataFrame predictions  = model.transform(testData);
//		predictions.show();
		
		DataFrame resultDF = predictions.groupBy("favorparent_category_id","parent_category_id").agg(ImmutableMap.of("prediction", "avg", "favorparent_category_id", "count"));
//		DataFrame resultDF =trainingData.groupBy("favorparent_category_id","parent_category_id").agg(ImmutableMap.of("rate", "avg", "favorparent_category_id", "count"));
		resultDF.toJavaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
			@Override
			public void call(Iterator<Row> t) throws Exception {
				MysqlUtil.save(t);
			}
		});
		
		resultDF.show();
		
		
		RegressionEvaluator evaluator = new RegressionEvaluator()
		  .setLabelCol("rate")
		  .setPredictionCol("prediction")
		  .setMetricName("rmse");
		double rmse = evaluator.evaluate(predictions);
		System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);
//
//		RandomForestRegressionModel rfModel = (RandomForestRegressionModel)(model.stages()[1]);
//		System.out.println("Learned regression forest model:\n" + rfModel.toDebugString());
//		 DataFrame lastResultDf = resultDf.groupBy("favorparent_category_id","parent_category_id").sum("rate");
//		 
//		 lastResultDf.show();
		 userProductFavorDF.unpersist();
		 context.stop();
		 
	}
}
