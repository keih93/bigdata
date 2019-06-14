import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class FactCountBolt extends NoisyBolt {
    private Map<String, Integer> counts = new HashMap<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(getIDs() + " executes tuple: " + tuple);

        String url = tuple.getString(1);
        String hourbucket = tuple.getString(2);

        Integer count = counts.get(url+hourbucket);
        if (count == null) {
            count = 0;
        }
        count++;
        counts.put(url+hourbucket, count);

        Values values = new Values(url, hourbucket, count);
        System.out.println(getIDs() + " total count: " + values);

        collector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "hourbucket", "count"));
    }
}
