package com.prateek.storm.nextgen.topologies;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.shade.org.apache.http.util.Asserts;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;

import java.security.InvalidParameterException;
import java.util.Map;

public class DRPCTopology  {


    public static class DRPCBolt extends BaseRichBolt{

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {

            String[] numbers = input.getString(1).split("\\+");
            Integer added = 0;
            if(numbers.length < 2){
                throw  new InvalidParameterException("Should be at least 2 numbers");
            }

            for(String i : numbers) {
                added += Integer.parseInt(i);
            }
            collector.emit(new Values(input.getValue(0),added));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id","result"));

        }
    }

    public static void main(String[] args) {

        LocalDRPC localDRPC = new LocalDRPC();


        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("add");
        builder.addBolt(new DRPCBolt(),1);

        Config config = new Config();
        //config.setDebug(true);

        LocalCluster localCluster = new LocalCluster();
        localCluster.
                submitTopology("drpc-adder-topology", config, builder.createLocalTopology(localDRPC));

        String result = localDRPC.execute("add", "1+1+10");

        System.out.println("result after executing drpc topology "+result);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        localCluster.shutdown();

        localDRPC.shutdown();

    }

}
