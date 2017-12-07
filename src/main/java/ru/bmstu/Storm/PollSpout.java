package ru.bmstu.Storm;

import com.google.common.base.Charsets;
import org.apache.storm.shade.com.google.common.io.Files;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;

import static org.apache.storm.utils.Utils.sleep;

public class PollSpout extends BaseRichSpout {
    public static final String POLL_DIR = "/home/trishlex/IdeaProjects/Storm/src/main/resources/in/";
    public static final String PROCESSED_DIR = "/home/trishlex/IdeaProjects/Storm/src/main/resources/out";
    public static final String SYNC_STREAM = "sync";
    public static final String WORDS_STREAM = "words";
    public static final String WORDS_FIELD = "words";

    private SpoutOutputCollector spoutOutputCollector;
    private boolean isReading;
    private File dir, currentFile;
    private int ackCount, sendCount;
    private BufferedReader reader;

    public PollSpout() {
        ackCount = 0;
        sendCount = 0;
        isReading = false;
        dir = new File(POLL_DIR);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(WORDS_STREAM, new Fields(WORDS_FIELD));
        outputFieldsDeclarer.declareStream(SYNC_STREAM, new Fields());
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void nextTuple() {
        if (isReading) {

            try {
                String line = reader.readLine();

                if (line != null) {
                    spoutOutputCollector.emit(WORDS_STREAM, new Values(line), sendCount);
                    sendCount++;
                }

                else {
                    if (ackCount == sendCount) {
                        File dest = new File(PROCESSED_DIR + currentFile.getName());
                        Files.move(currentFile, dest);

                        spoutOutputCollector.emit(SYNC_STREAM, new ArrayList<Object>());
                        ackCount = 0;
                        sendCount = 0;

                        isReading = false;
                    }
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {

            File files[] = dir.listFiles();

            if (files == null || files.length == 0) {
                sleep(100);
            }

            else {

                try {
                    currentFile = files[0];
                    reader = new BufferedReader
                            (new InputStreamReader
                                    (new FileInputStream(currentFile), Charsets.UTF_8));
                }
                catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                finally {
                    isReading = true;
                }
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        ackCount++;
    }

    @Override
    public void fail(Object msgId) {
        throw new RuntimeException("ack is failed");
    }
}
