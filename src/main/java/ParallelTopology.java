import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.apache.storm.LocalCluster;

public class ParallelTopology {
    public static double e;
    public static double th = 0.05;

    public static void main(String[] args) throws Exception {

        Config config = new Config();

        config.setDebug(true);
        config.setNumWorkers(4);

        TopologyBuilder builder = new TopologyBuilder();

        e = 0.06;
        builder.setSpout("TwitterParallelSpout", new ParallelSprout(),3);
        ParallelAlgo plc = new ParallelAlgo(e, th);
        builder.setBolt("ParallelAlgo", plc,3).fieldsGrouping("TwitterParallelSpout",new Fields("hashTag"));
        builder.setBolt("ParallelOut", new ParallelOut(),1).globalGrouping("ParallelAlgo");

        StormSubmitter.submitTopology("TwitterParallelStream", config, builder.createTopology());

//        LocalCluster cluster = new LocalCluster();
//
//        cluster.submitTopology("test", config, builder.createTopology());

        Utils.sleep(10000);
    }
}
