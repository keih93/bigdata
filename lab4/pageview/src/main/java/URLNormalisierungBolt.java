import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class URLNormalisierungBolt extends NoisyBolt {
    private Map<String, Integer> counts = new HashMap<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(getIDs() + " executes tuple: " + tuple);

        String urlStr = tuple.getString(1);
        String normalizedURL = "";
        try {
            URL url = new URL(urlStr);
            normalizedURL = url.getProtocol() + "://" + url.getHost() + url.getPath();
        } catch (MalformedURLException e) {
            System.err.println("MalformedURLException");
        }

        Integer count = counts.get(normalizedURL);
        if (count == null) {
            count = 0;
        }
        count++;
        counts.put(normalizedURL, count);

        Values values = new Values(normalizedURL, count);
        System.out.println(getIDs() + " result values: " + values);

        collector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
