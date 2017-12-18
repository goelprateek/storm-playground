package com.prateek.storm.nextgen.topologies;

import com.prateek.storm.nextgen.spouts.MongoReader;
import junit.framework.TestCase;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.testing.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.Map;
import java.util.Set;

public class MongoTopologyTest extends TestCase {


    public void testMongoTopology() {

        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        String inputCollectionName = "public";
        String outputCollectionName = "output";
        String url = "mongodb://localhost:27017/test";

        MongoMapper mapper = new SimpleMongoMapper()
                .withFields("x","y");

        Testing.withSimulatedTimeLocalCluster(new TestJob() {
            @Override
            public void run(ILocalCluster cluster) {
                Date date = new Date();
                TopologyBuilder builder = new TopologyBuilder();

                AckFailMapTracker tracker = new AckFailMapTracker();

                MongoReader spout = new MongoReader(url, inputCollectionName);

                builder.setSpout("MongoReader", spout, 1);
                builder.setBolt("MongoWriter", new MongoInsertBolt(url,outputCollectionName,mapper), 1)
                        .shuffleGrouping("MongoReader");

                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("MongoReader", new Values("1", date),
                        new Values("2", date), new Values("3", date));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                conf.setMessageTimeoutSecs(10);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, builder.createTopology(),
                        completeTopologyParam);

                System.out.println(Testing.readTuples(result,"MongoWriter"));

                assertTrue(Testing.multiseteq(new Values(new Values("1", date),
                        new Values("2",date), new Values("3",date)),
                        Testing.readTuples(result, "MongoReader")));




            }
        });
    }

}