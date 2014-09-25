package com.ganqiang.recsys.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.ganqiang.recsys.hbase.HConnectionController;

public final class Constants implements Initializable{

	private static final Logger logger = Logger.getLogger(Constants.class);

	private static Properties p = new Properties();
	
	public final static String test_log = "{\"data\":{\"access_time\":\"2014-09-11 11:11:11\",\"item_id\":\"item_01\"},\"from\":\"www.baidu.com\",\"url\":\"www.baidu.com\",\"session_id\":\"session_id\",\"cookies\":\"cookies_info\",\"language\":\"english\",\"bs\":\"ie\",\"client_id\":\"client-223\",\"action\":\"action-0\"}";

	public final static String[] hbase_tables = new String[]{"USER_ACTION_LOG", "USER_PREF", "SCORE"};
	public final static String	hbase_user_action_log_table = "USER_ACTION_LOG";
	public final static String	hbase_user_pref_table = "USER_PREF";
	public final static String	hbase_score_table = "SCORE";
	public final static String	hbase_column_family = "cf";
	
	public static String	hbase_zk_quorum = "localhost";
	public static String	hbase_master = "";
	public static Integer hbase_zk_client_port = 2182;
	
	public static String kafka_zk_address = "localhost:2183";
	public static String kafka_topic = "topic-log";
	public static String kafka_zk_root = "kafka-spout";
	public static String kafka_id = "kafkaspout";
	public static String kakfa_broker_list = "localhost:9092";
	
	public static String storm_topology_name = "Topo";
	
	public static HConnectionController hbase_con = null;
	
	public void init() {
		init(System.getProperty("user.dir") + "/conf/log4j.conf");
		init(System.getProperty("user.dir") + "/conf/system.conf");
		PropertyConfigurator.configure(p);
		
		logger.info("configuration files init finish.");
	}

	private void init(String propertyFileName) {
		InputStream in = null;
		try {
			in = new FileInputStream(propertyFileName);
			if (in != null){
				p.load(in);
			}
		} catch (IOException e) {
			logger.error("load " + propertyFileName + " into Contants error");
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public String getProperty(String key, String defaultValue) {
		return p.getProperty(key, defaultValue);
	}

}
