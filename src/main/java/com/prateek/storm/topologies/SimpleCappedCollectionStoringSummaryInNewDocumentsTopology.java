package com.prateek.storm.topologies;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.prateek.storm.bolts.MongoInsertBolt;
import com.prateek.storm.core.MongoObjectGrabber;
import com.prateek.storm.core.StormMongoObjectGrabber;
import com.prateek.storm.spouts.MongoCappedCollectionSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SimpleCappedCollectionStoringSummaryInNewDocumentsTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        // Map the mongodb object to a tuple
        MongoObjectGrabber mongoMapper = new MongoObjectGrabber() {
            @Override
            public List<Object> map(DBObject object) {
                List<Object> tuple = new ArrayList<Object>();
                // Add the a variable
                tuple.add(object.get("x"));
                // Return the mapped object
                return tuple;
            }

            @Override
            public String[] fields() {
                return new String[]{"x"};
            }
        };


        // Field mapper
        StormMongoObjectGrabber mapper = new StormMongoObjectGrabber() {
            @Override
            public DBObject map(DBObject object, Tuple tuple) {
                return BasicDBObjectBuilder.start()
                        .add("sum", tuple.getIntegerByField("sum"))
                        .add("timestamp", new Date())
                        .get();
            }
        };

        // Build a topology
        TopologyBuilder builder = new TopologyBuilder();

        // Set the spout
        builder.setSpout("mongodb", new MongoCappedCollectionSpout("mongodb://localhost:27017/test", "public", mongoMapper), 1);
        // Add a bolt
        builder.setBolt("sum", new SummarizerBolt(), 1).allGrouping("mongodb");
        builder.setBolt("mongo",
                new MongoInsertBolt("mongodb://localhost:27017/test",
                        "output",
                        mapper,
                        WriteConcern.JOURNALED,
                        true)
                , 1).allGrouping("sum");

        // Set debug config
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumEventLoggers(2);

        // Run on local cluster
        //LocalCluster cluster = new LocalCluster();
        // Submit the topology
        //cluster.submitTopology("test2", conf, builder.createTopology());
        // Starts inserts


        StormSubmitter.submitTopologyWithProgressBar("test2", conf, builder.createTopology());

        // Stop topology
        //cluster.killTopology("test2");
        //cluster.shutdown();

        // Ensure we die
        System.exit(0);

    }


    // Bolt summarising numbers
    static class SummarizerBolt implements IBasicBolt {
        static Logger LOG = LoggerFactory.getLogger(SummarizerBolt.class);
        private int sum = 0;
        private int numberOfRecords = 0;

        public void prepare(Map map, TopologyContext topologyContext) {
        }

        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            // Add to sum

            this.sum = this.sum + tuple.getIntegerByField("x");
            this.numberOfRecords = this.numberOfRecords + 1;
            // Create tuple list
            List<Object> tuples = new ArrayList<Object>();
            // Add sum as tuple
            tuples.add(this.sum);
            // Emit transformed tuple
            basicOutputCollector.emit(tuples);
        }

        public void cleanup() {
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sum"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
}
