package ru.bmstu.Storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.StringTokenizer;

public class SplitBolt extends BaseRichBolt {
    public static final String WORD_FIELD = "word";

    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(WORD_FIELD));
    }

    public void execute(Tuple tuple) {
        String line = tuple.getStringByField(PollSpout.WORDS_FIELD);
        StringTokenizer st = new StringTokenizer(line, " \t\n\r,.:;?!\"—()[]„«»…");

        while(st.hasMoreTokens()){
            outputCollector.emit(tuple, new Values(st.nextToken()));
        }

        outputCollector.ack(tuple);
    }
}
