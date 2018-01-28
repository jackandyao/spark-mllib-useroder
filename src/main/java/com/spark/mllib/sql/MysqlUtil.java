package com.spark.mllib.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Row;


public class MysqlUtil {
	

	public static void save(Iterator<Row> r) {
		
		Connection con = null;
		PreparedStatement  pst = null;
		try{
			con = DriverManager.getConnection("jdbc:mysql://192.168.10.41:3306/test", "root", "huize123");
			String sql = "insert into favorite_insure(favoriteCategoryId,insureCategoryId,prediction,pcount)values(?,?,?,?)";
			pst = con.prepareStatement(sql);
			while(r.hasNext()){
				System.out.println("处理正在进行=====");
				Row rw = r.next();
				Integer favoriteCategoryId = Integer.valueOf(rw.get(0).toString());
				Integer insureCategoryId = Integer.valueOf(rw.get(1).toString());
				Double prediction = Double.valueOf(rw.get(2).toString());
				Long pcount = Long.valueOf(rw.get(3).toString());
				pst.setInt(1, favoriteCategoryId);
				pst.setInt(2, insureCategoryId);
				pst.setDouble(3, prediction);
				pst.setLong(4, pcount);
				pst.addBatch();
//				i++;
//				if(i %100==0){
//					pst.executeBatch();
//				}
				
			}
			pst.executeBatch();
//			pst.clearBatch();
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(con !=null){
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(pst !=null){
				try {
					pst.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		
	}

	

}
