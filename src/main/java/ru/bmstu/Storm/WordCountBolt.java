package ru.bmstu.Storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private HashMap<String, Integer> table;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        table = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(PollSpout.SYNC_STREAM)) {

            for (String key: table.keySet())
                System.out.println(key + " : " + table.get(key));

            table = new HashMap<String, Integer>();
        } else {

            String word = tuple.getStringByField(SplitBolt.WORD_FIELD);
            Integer count = table.get(word);

            if (count == null)
                count = 0;

            count++;
            table.put(word, count);

            outputCollector.ack(tuple);
        }
    }
}
