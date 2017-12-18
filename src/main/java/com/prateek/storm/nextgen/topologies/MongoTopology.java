package com.prateek.storm.nextgen.topologies;

import com.prateek.storm.nextgen.spouts.MongoReader;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.topology.TopologyBuilder;

public class MongoTopology {

    public static void main(String[] args) {

        String url = "mongodb://localhost:27017/test";
        String collectionName = "public";

        MongoMapper mapper = new SimpleMongoMapper()
                .withFields("x","y");

        MongoReader spout = new MongoReader(url, collectionName);

        MongoInsertBolt mongoBolt = new MongoInsertBolt(url, "output", mapper)
                .withOrdered(true)
                .withFlushIntervalSecs(10);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1" , spout, 1);
        builder.setBolt("2", mongoBolt, 1).shuffleGrouping("1");

        Config config = new Config();

        config.setDebug(true);

        config.setNumWorkers(1);

        try {

            StormSubmitter.submitTopologyWithProgressBar("MongoTopology",config, builder.createTopology());

        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }

    }

}
