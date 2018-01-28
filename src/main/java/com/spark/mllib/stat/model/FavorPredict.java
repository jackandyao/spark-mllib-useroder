package com.spark.mllib.stat.model;

public class FavorPredict {
	
	private Integer favoriteCategoryId ;
	private Integer insureCategoryId ;
	private Double prediction  ;
	private Long pcount ;
	
	
	public FavorPredict(Integer favoriteCategoryId, Integer insureCategoryId,
			Double prediction, Long pcount) {
		super();
		this.favoriteCategoryId = favoriteCategoryId;
		this.insureCategoryId = insureCategoryId;
		this.prediction = prediction;
		this.pcount = pcount;
	}
	
	
	public FavorPredict() {
	}


	public Integer getFavoriteCategoryId() {
		return favoriteCategoryId;
	}
	public void setFavoriteCategoryId(Integer favoriteCategoryId) {
		this.favoriteCategoryId = favoriteCategoryId;
	}
	public Integer getInsureCategoryId() {
		return insureCategoryId;
	}
	public void setInsureCategoryId(Integer insureCategoryId) {
		this.insureCategoryId = insureCategoryId;
	}
	public Double getPrediction() {
		return prediction;
	}
	public void setPrediction(Double prediction) {
		this.prediction = prediction;
	}
	public Long getPcount() {
		return pcount;
	}
	public void setPcount(Long pcount) {
		this.pcount = pcount;
	}
	
	

}
