import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Arrays;

public class DataExtractBolt extends NoisyBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(getIDs() + " executes tuple: " + tuple);

        String sentence = tuple.getString(4);
//        collector.emit(new Values(sentence));
//        Arrays.stream(sentence.split("\\s"))
//                .forEach(word -> collector.emit(new Values(word)));

        String[] datas = sentence.split("\\s");
        String ip, url, epochTime;
        if (datas.length >= 3) {
            Values values = new Values(datas[0], datas[1], datas[2]);
            collector.emit(values);
        } else {
            System.err.println("Input must be int the form <IP> <URL> <EPOCH-TIME>");
            Values values = new Values("error", "error", "error");
        }
        collector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url"));
    }
}
