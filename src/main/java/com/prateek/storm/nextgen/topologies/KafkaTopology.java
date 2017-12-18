package com.prateek.storm.nextgen.topologies;

import com.prateek.storm.nextgen.spouts.MongoReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class KafkaTopology {


    public static void main(String[] args) {

        String zkConnString = "localhost:2181";
        BrokerHosts hosts = new ZkHosts(zkConnString);


        SpoutConfig spoutConfig = new SpoutConfig(hosts, "test", "/" + "test", UUID.randomUUID().toString());


        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        MongoMapper mapper = new SimpleMongoMapper()
                .withFields("word","count");


        MongoInsertBolt mongoBolt = new MongoInsertBolt("mongodb://localhost:27017/test", "output", mapper)
                .withOrdered(true)
                .withFlushIntervalSecs(10);

        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", kafkaSpout, 1);
        tp.setBolt("splitter", new SplitSentence(), 1).shuffleGrouping("kafka_spout");
        tp.setBolt("count", new WordCount(), 1).shuffleGrouping("splitter");
        tp.setBolt("mongo_insert", mongoBolt, 1 ).shuffleGrouping("count");


        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1);
        config.setNumWorkers(1);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("kafka-test", config, tp.createTopology());

    }

    public static class SplitSentence extends BaseBasicBolt{

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String sentence = tuple.getString(0);
            for (String word: sentence.split("\\s+")) {
                collector.emit(new Values(word, 1));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

    }

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

}
