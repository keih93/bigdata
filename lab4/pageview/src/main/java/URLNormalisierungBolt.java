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

        String word = tuple.getString(1);
//        String urlStr = page.get_url();
//        try {
//            URL url = new URL(urlStr);
//            page.set_url(url.getProtocol() + "://" + url.getHost() + url.getPath());
//        } catch (MalformedURLException e) {
//        }

        Integer count = counts.get(word);
        if (count == null) {
            count = 0;
        }
        count++;
        counts.put(word, count);

        Values values = new Values(word, count);
        System.out.println(getIDs() + " result values: " + values);

        collector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
