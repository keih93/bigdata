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
        String word = "random";
        collector.emit(new Values(word));
//        Arrays.stream(sentence.split("\\s"))
//                .forEach(word -> collector.emit(new Values(word)));

//        ArrayList<String> datas = new ArrayList<String>();
//        Arrays.stream(sentence.split("\\s"))
//                .forEach(word -> datas.add(word));
//        if(datas.size()>3) {
//            System.out.println(datas);
//            String word = datas.get(1) + datas.get(2);
//            collector.emit(new Values(word));
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url"));
    }
}
