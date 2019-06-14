import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HourBucketBolt extends NoisyBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(getIDs() + " executes tuple: " + tuple);

        String time = tuple.getString(2);
        int intime = Integer.parseInt(time);

        String sdate = new SimpleDateFormat("yyyy-MM-dd/HH").format(new Date(intime*1000L));

        Values values = new Values(tuple.getString(0),tuple.getString(1), sdate);

        System.out.println(getIDs() + " result values: " + values);

        collector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "normalizedURL", "hourbucket"));
    }
}
