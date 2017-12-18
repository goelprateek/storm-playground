package com.prateek.storm;

import junit.framework.TestCase;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class TestStromApi extends TestCase {

    public void testWithLocalCluster(){
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(2);
        mkClusterParam.setPortsPerSupervisor(5);
        Config daemonConf = new Config();
        daemonConf.put(Config.SUPERVISOR_ENABLE, false);
        daemonConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);

        Testing.withLocalCluster(mkClusterParam, cluster -> assertNotNull(cluster.getState()));
    }

    public void testBasicTopology(){
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws Exception {
                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("1", new TestWordSpout(true), 3);
                builder.setBolt("2", new TestWordCounter(), 4).fieldsGrouping(
                        "1", new Fields("word"));
                builder.setBolt("3", new TestGlobalCount()).globalGrouping("1");
                builder.setBolt("4", new TestAggregatesCounter())
                        .globalGrouping("2");
                StormTopology topology = builder.createTopology();

                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("1", new Values("prateek"),
                        new Values("rohit"), new Values("sandy"), new Values(
                                "prateek"));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                /**
                 * TODO
                 */
                Map result = Testing.completeTopology(cluster, topology,
                        completeTopologyParam);

                System.out.println(Testing.readTuples(result, "1"));

                // check whether the result is right
                assertTrue(Testing.multiseteq(new Values(new Values("prateek"),
                        new Values("rohit"), new Values("sandy"), new Values(
                        "prateek")), Testing.readTuples(result, "1")));
                assertTrue(Testing.multiseteq(new Values(new Values("prateek", 1),
                        new Values("prateek", 2), new Values("sandy", 1),
                        new Values("rohit", 1)), Testing.readTuples(result, "2")));
                assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2),
                        new Values(3), new Values(4)), Testing.readTuples(
                        result, "3")));
                assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2),
                        new Values(3), new Values(4)), Testing.readTuples(
                        result, "4")));

            }
        });
    }
}
