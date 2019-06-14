import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class HourBucketBolt extends NoisyBolt {
    private Map<String, Integer> counts = new HashMap<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(getIDs() + " executes tuple: " + tuple);

        String time = tuple.getString(2);

        int hourbucket = Integer.parseInt(time)/(60*60);

        Values values = new Values(tuple.getString(0),tuple.getString(1), hourbucket);
        System.out.println(getIDs() + " result values: " + values);

        collector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
