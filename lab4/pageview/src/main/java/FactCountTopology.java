import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class FactCountTopology {

    private static StormTopology buildTopology(){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka_spout", new PageViewSpout(), 1);

        builder.setBolt("dataextract_bolt", new DataExtractBolt(), 2)
                .shuffleGrouping("kafka_spout", PageViewSpout.STREAM_NAME);

        builder.setBolt("urlnormalize_bolt", new URLNormalisierungBolt(), 2)
                .fieldsGrouping("dataextract_bolt",  new Fields("ip","url","epochtime"));

        builder.setBolt("hourbucket_bolt", new HourBucketBolt(), 2)
                .fieldsGrouping("urlnormalize_bolt",  new Fields("ip", "normalizedURL","epochtime"));
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        StormTopology topology = buildTopology();

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", conf, topology);
            try {
                System.out.println("PRESS ENTER TO STOP");
                new BufferedReader(new InputStreamReader(System.in)).readLine();
                cluster.killTopology("wordcount");
                cluster.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}