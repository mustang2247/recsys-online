package com.ganqiang.recsys.bolt;

import com.ganqiang.recsys.entity.UserActionLog;
import com.ganqiang.recsys.hbase.HBaseStore;
import com.ganqiang.recsys.util.JsonHelper;
import com.ganqiang.recsys.util.StringUtil;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

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