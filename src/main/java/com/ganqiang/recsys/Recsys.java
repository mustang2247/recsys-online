package com.ganqiang.recsys;

import com.ganqiang.recsys.bolt.HBaseStoreBolt;
import com.ganqiang.recsys.spout.MessageScheme;
import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.Initializer;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Recsys {

	private static final Logger logger = Logger.getLogger(Recsys.class);
	private static final boolean islocal = true;

	static {
		Initializer.setup();
	}

	public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
		logger.info("begin to running recsys.");
		BrokerHosts brokerHosts = new ZkHosts(Constants.kafka_zk_address);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, Constants.kafka_topic, 	Constants.kafka_zk_root, Constants.kafka_id);

		Config conf = new Config();
		Map<String, String> map = new HashMap<String, String>();
		map.put("metadata.broker.list", Constants.kakfa_broker_list);
		map.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put("kafka.broker.properties", map);
//		conf.put("topic", "topic2");

		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig));
		builder.setBolt("bolt", new HBaseStoreBolt()).shuffleGrouping("spout");
//		builder.setBolt("kafkabolt", new KafkaBolt<String, Integer>()).shuffleGrouping("bolt");

		if (!islocal) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(Constants.storm_topology_name, conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(Constants.storm_topology_name, conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology(Constants.storm_topology_name);
			cluster.shutdown();
		}
		logger.info("run recsys finish.");
	}

	public static void clientproduce(){
		Properties props = new Properties();
		props.put("zk.connect", "localhost:2183");
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		String msg = Constants.test_log;
//		ProducerConfig config = new ProducerConfig(props);
//		Producer<String, String> producer = new Producer<String, String>(config);
//		KeyedMessage<String, String> data = new KeyedMessage<String, String>("topic1",  msg);
//		producer.send(data);
//		producer.close();
	}

}
