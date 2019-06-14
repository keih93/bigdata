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
		if(datas.length > 3){
			String word = datas[1] + datas[2];
		}else{
			String word = sentence;
		}
		collector.emit(new Values(word));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
	}
}
