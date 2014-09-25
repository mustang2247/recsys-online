package com.ganqiang.recsys.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.ganqiang.recsys.entity.UserActionLog;
import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.ModelNormalizer;
import com.ganqiang.recsys.util.StringUtil;


public final class HBaseStore {

	private static final Logger logger = Logger.getLogger(HBaseStore.class);

	public static void writeUserActionLog(UserActionLog ualog) {
		HTableInterface table = null;
		String columnfamily = Constants.hbase_column_family;
		try {
			table = Constants.hbase_con.getHTableInterface(Constants.hbase_user_action_log_table);
			Put p = new Put(Bytes.toBytes(StringUtil.MD5(ualog.getUrl())));
			p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("ACCESS_TIME"), 
					Bytes.toBytes(ualog.getAccessTime()));
			p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("CLIENT_ID"),
					Bytes.toBytes(ualog.getClientId()));
			if (!StringUtil.isNullOrBlank(ualog.getUrl())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("URL"),
						Bytes.toBytes(ualog.getItemId()));
			}
			if (!StringUtil.isNullOrBlank(ualog.getItemId())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("ITEM_ID"),
						Bytes.toBytes(ualog.getItemId()));
			}
			p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("COOKIES"), 
					Bytes.toBytes(ualog.getCookies()));
			if (!StringUtil.isNullOrBlank(ualog.getAction())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("ACTION"),
						Bytes.toBytes(ualog.getAction()));
			}
			if (!StringUtil.isNullOrBlank(ualog.getBs())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("BS"),
						Bytes.toBytes(ualog.getBs()));
			}
			if (!StringUtil.isNullOrBlank(ualog.getSessionId())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("SEESION_ID"),
						Bytes.toBytes(ualog.getSessionId()));
			}
			if (!StringUtil.isNullOrBlank(ualog.getLanguage())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("LANGUAGE"),
						Bytes.toBytes(ualog.getLanguage()));
			}
			if (!StringUtil.isNullOrBlank(ualog.getKey())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("KEY"),
						Bytes.toBytes(ualog.getKey()));
			}
			if (!StringUtil.isNullOrBlank(ualog.getScore())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("SCORE"),
						Bytes.toBytes(ualog.getScore()));
			}
			table.put(p);
		} catch (IOException e) {
			logger.error("Cannot to excute writeBatch method by hbase ", e);
		} finally {
			try {
				if (table != null) {
					table.close();
				}
			} catch (IOException e) {
				logger.error("Cannot to excute writeBatch method by hbase  ", e);
			}
		}
	}
	
	public static void writeUserPref(UserActionLog ualog) {
		HTableInterface table = null;
		String columnfamily = Constants.hbase_column_family;
		try {
			table = Constants.hbase_con.getHTableInterface(Constants.hbase_user_pref_table);
			Put p = new Put(Bytes.toBytes(System.currentTimeMillis() + StringUtil.MD5(ualog.getClientId())));
			if (!StringUtil.isNullOrBlank(ualog.getClientId())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("CLIENT_ID"),
						Bytes.toBytes(ualog.getClientId()));
			}
			if (!StringUtil.isNullOrBlank(ualog.getAprPref())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("APR_NORM"),
						Bytes.toBytes(ModelNormalizer.getYearRateNorm(Double.valueOf(ualog.getAprPref()))));
			}
			if (!StringUtil.isNullOrBlank(ualog.getAction())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("PER_NORM"),
						Bytes.toBytes(ModelNormalizer.getRepayLimitTimeNorm(ualog.getPerPref())));
			}
			table.put(p);
		} catch (IOException e) {
			logger.error("Cannot to excute writeBatch method by hbase ", e);
		} finally {
			try {
				if (table != null) {
					table.close();
				}
			} catch (IOException e) {
				logger.error("Cannot to excute writeBatch method by hbase  ", e);
			}
		}
	}
	
	
	public static void writeScore(UserActionLog ualog) {
		HTableInterface table = null;
		String columnfamily = Constants.hbase_column_family;
		try {
			table = Constants.hbase_con.getHTableInterface(Constants.hbase_score_table);
			Put p = new Put(Bytes.toBytes(System.currentTimeMillis() + StringUtil.MD5(ualog.getClientId())));
			if (!StringUtil.isNullOrBlank(ualog.getClientId())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("CLIENT_ID"),
						Bytes.toBytes(ualog.getClientId()));
			}
			if (!StringUtil.isNullOrBlank(ualog.getItemId())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("ITEM_ID"),
						Bytes.toBytes(ualog.getItemId()));
			}
			if (!StringUtil.isNullOrBlank(ualog.getScore())) {
				p.add(Bytes.toBytes(columnfamily), Bytes.toBytes("SCORE"),
						Bytes.toBytes(ModelNormalizer.getRepayLimitTimeNorm(ualog.getPerPref())));
			}
			table.put(p);
		} catch (IOException e) {
			logger.error("Cannot to excute writeBatch method by hbase ", e);
		} finally {
			try {
				if (table != null) {
					table.close();
				}
			} catch (IOException e) {
				logger.error("Cannot to excute writeBatch method by hbase  ", e);
			}
		}
	}

}
