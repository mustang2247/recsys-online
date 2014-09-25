package com.ganqiang.recsys.bolt;

import com.ganqiang.recsys.entity.UserActionLog;
import com.ganqiang.recsys.hbase.HBaseStore;
import com.ganqiang.recsys.util.JsonHelper;
import com.ganqiang.recsys.util.StringUtil;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HBaseStoreBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 9158412709377600779L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		String jsons = (String) input.getValue(0);
		UserActionLog log = JsonHelper.format(jsons);
		HBaseStore.writeUserActionLog(log);
		String action = log.getAction();
		if (StringUtil.isNullOrBlank(action)) {
			if(action.equals("0")){
				HBaseStore.writeUserPref(log);
			}else if(action.equals("4")){
				HBaseStore.writeScore(log);
			}
		}
		collector.emit(new Values("ok"));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}
}