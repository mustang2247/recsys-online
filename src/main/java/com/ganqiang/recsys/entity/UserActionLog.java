package com.ganqiang.recsys.entity;

public class UserActionLog {
	private String accessTime;
	private String itemId;
	private String from;
	private String url;
	private String sessionId;
	private String cookies;
	private String language;
	private String bs;// js获取到的浏览器
	private String clientId;// 客户端唯一标识用户ID
	private String action;//注册：0，登陆：1，浏览：2，收藏：3，评分：4，搜索：5
	private String key;//关键字
	private String score;//评分
	private String aprPref;//年利率的偏好
	private String perPref;//期限的偏好

	public String getAprPref() {
		return aprPref;
	}
	public void setAprPref(String aprPref) {
		this.aprPref = aprPref;
	}
	public String getPerPref() {
		return perPref;
	}
	public void setPerPref(String perPref) {
		this.perPref = perPref;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getScore() {
		return score;
	}
	public void setScore(String score) {
		this.score = score;
	}
	public String getAccessTime() {
		return accessTime;
	}
	public void setAccessTime(String accessTime) {
		this.accessTime = accessTime;
	}
	public String getItemId() {
		return itemId;
	}
	public void setItemId(String itemId) {
		this.itemId = itemId;
	}
	public String getFrom() {
		return from;
	}
	public void setFrom(String from) {
		this.from = from;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public String getCookies() {
		return cookies;
	}
	public void setCookies(String cookies) {
		this.cookies = cookies;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public String getBs() {
		return bs;
	}
	public void setBs(String bs) {
		this.bs = bs;
	}
	public String getClientId() {
		return clientId;
	}
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public String getAction() {
		return action;
	}
	public void setAction(String action) {
		this.action = action;
	}

}
