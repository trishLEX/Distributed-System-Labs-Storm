package ru.bmstu.Storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class StormStarter {
    public static final String GENERATOR = "generator";
    public static final String SYNC = "sync";
    public static final String WORDS = "words";
    public static final String WORD_FIELD = "word";
    public static final String SPLITTER_BOLT = "splitter";
    public static final String COUNTER_BOLT = "counter";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(GENERATOR, new PollSpout());

        builder.setBolt(SPLITTER_BOLT, new SplitBolt(), 10)
                .shuffleGrouping(GENERATOR, WORDS);

        builder.setBolt(COUNTER_BOLT, new WordCountBolt(), 1)
                .fieldsGrouping(SPLITTER_BOLT, new Fields(WORD_FIELD))
                .allGrouping(GENERATOR, SYNC);

        Config conf = new Config();

        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("FrequencyDictionary", conf, builder.createTopology());
    }
}
