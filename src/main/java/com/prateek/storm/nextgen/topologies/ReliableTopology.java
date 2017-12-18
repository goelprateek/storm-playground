package com.prateek.storm.nextgen.topologies;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ReliableTopology {

    public static class ReliableSpout extends BaseRichSpout{
        private static final Logger LOGGER = LoggerFactory.getLogger(ReliableSpout.class);

        private SpoutOutputCollector  collector ;
        private Map<Integer,String> toSend;
        private Map<Integer,Integer> transectionFailCount;
        private Map<Integer,String> messages;
        private final int MAX_FAILURE = 2;

        @Override
        public void ack(Object msgId) {
            messages.remove(msgId);
            transectionFailCount.remove(msgId);
            LOGGER.info("messages successfully processed [{}]", msgId);
        }

        @Override
        public void fail(Object msgId) {

            if(!transectionFailCount.containsKey(msgId)){
                throw new RuntimeException("Error transaction Id not found ");
            }

            Integer  transactionId =  (Integer)msgId;

            Integer failure = transectionFailCount.get(transactionId);

            if(failure > MAX_FAILURE){
                throw new RuntimeException("Maximum failure reached");
            }

            transectionFailCount.put(transactionId,failure);
            toSend.put(transactionId,messages.get(transactionId));

            LOGGER.info("Resending message");

        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.messages = new HashMap<>();
            this.transectionFailCount = new HashMap<>();
            this.toSend = new HashMap<>();
            Random random = new Random();

            for(int i=0;i<100;i++){
                messages.put(i,"transaction_"+random.nextInt());
                transectionFailCount.put(i,0);
            }
            toSend.putAll(messages);
            this.collector = collector;
        }

        @Override
        public void nextTuple() {

            if(!toSend.isEmpty()){
                for(Map.Entry<Integer, String> map : toSend.entrySet()){
                    Integer transactionId = map.getKey();
                    String transactionMessage = map.getValue();
                    collector.emit(new Values(transactionMessage),transactionId);
                }

                toSend.clear();
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("transactionMessage"));
        }
    }

    public static class ReliableBolt extends BaseRichBolt{

        private final Integer MAX_FAIL = 80;
        private  OutputCollector collector;
        private Random random = new Random();

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            int i = random.nextInt(100);
            if(i > MAX_FAIL){
                collector.ack(input);
            }else{
                collector.fail(input);
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("reliableSpout", new ReliableSpout());
        topologyBuilder.setBolt("reliableBolt", new ReliableBolt()).shuffleGrouping("reliableSpout");

        LocalCluster localCluster = new LocalCluster();
        Config config = new Config();
        config.setDebug(true);
        localCluster.submitTopology("reliableTopology", config,topologyBuilder.createTopology());

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        localCluster.shutdown();

    }
}
