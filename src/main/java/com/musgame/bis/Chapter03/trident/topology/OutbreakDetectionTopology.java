package com.musgame.bis.Chapter03.trident.topology;

import com.musgame.bis.Chapter03.trident.operator.*;
import com.musgame.bis.Chapter03.trident.spout.DiagnosisEventSpout;
import com.musgame.bis.Chapter03.trident.state.OutbreakTrendFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class OutbreakDetectionTopology {

    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        DiagnosisEventSpout spout = new DiagnosisEventSpout();
        Stream inputStream = topology.newStream("event", spout);

        // Filter for critical events 过滤关键事件
        inputStream.each(new Fields("event"), new DiseaseFilter())

                // Locate the closest city 找到最近的城市
                .each(new Fields("event"), new CityAssignment(), new Fields("city"))

                // Derive the hour segment 导出小时段
                .each(new Fields("event", "city"), new HourAssignment(),
                        new Fields("hour", "cityDiseaseHour"))

                // Group occurrences in same city and hour 在同一城市按小时发生分组
                .groupBy(new Fields("cityDiseaseHour"))

                // Count occurrences and persist the results 计算发现次数并持久保存结果
                .persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count"))
                .newValuesStream()

                // Detect an outbreak 检测发病
                .each(new Fields("cityDiseaseHour", "count"),
                        new OutbreakDetector(),
                        new Fields("alert"))

                // Dispatch the alert 发出警报
                .each(new Fields("alert"), new DispatchAlert(), new Fields());
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc", conf, buildTopology());
        Thread.sleep(200000);
        cluster.shutdown();
    }
}
