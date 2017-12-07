package ru.bmstu.Storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class StormStarter {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("generator", new PollSpout());

        builder.setBolt("splitter", new SplitBolt(), 10).shuffleGrouping("generator", "words");

        builder.setBolt("counter", new WordCountBolt(), 1)
                .fieldsGrouping("splitter", new Fields("word"))
                .allGrouping("generator", "sync");

        Config conf = new Config();

        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("FrequencyDictionary", conf, builder.createTopology());
    }
}
