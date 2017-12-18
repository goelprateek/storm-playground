package com.prateek.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;

public class Application {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("word", new TestWordSpout(), 3);

        /*topologyBuilder.setBolt("exclaim1", new ExclamationBolt()).shuffleGrouping("word");
        topologyBuilder.setBolt("exclaim2", new ExclamationBolt()).shuffleGrouping("exclaim1");*/

        Config conf = new Config();

        conf.setDebug(true);
        conf.setNumWorkers(3);
        conf.setNumEventLoggers(2);


        if(null != args && args.length > 0) {
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topologyBuilder.createTopology());
        }else{
            StormSubmitter.submitTopologyWithProgressBar("test", conf, topologyBuilder.createTopology());
            /*LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("test", conf, topologyBuilder.createTopology());
            Utils.sleep(10000);
            localCluster.killTopology("test");
            localCluster.shutdown();*/
        }


    }
}
