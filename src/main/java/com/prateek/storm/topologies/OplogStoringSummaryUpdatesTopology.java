package com.prateek.storm.topologies;

import com.mongodb.*;
import com.prateek.storm.bolts.MongoUpdateBolt;
import com.prateek.storm.core.MongoObjectGrabber;
import com.prateek.storm.core.StormMongoObjectGrabber;
import com.prateek.storm.core.UpdateQueryCreator;
import com.prateek.storm.spouts.MongoOpLogSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
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
import org.bson.BSONObject;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OplogStoringSummaryUpdatesTopology {

    static Logger logger = LoggerFactory.getLogger(OplogStoringSummaryUpdatesTopology.class);

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Mongo mongo = new MongoClient("localhost", 37017);

        logger.info(" starting connection with mongo  ");

        // Signals thread to fire messages
        //CountDownLatch latch = new CountDownLatch(1);


        // Connect to the db and find the current last timestamp
        DB db = mongo.getDB("local");
        DBObject query = null;
        DBCursor cursor = db.getCollection("oplog.$main").find().sort(new BasicDBObject("$natural", -1)).limit(1);
        if (cursor.hasNext()) {
            // Get the next object
            DBObject object = cursor.next();
            // Build the query
            query = new BasicDBObject("ts", new BasicDBObject("$gt", object.get("ts")));
        }

        // Build a topology
        TopologyBuilder builder = new TopologyBuilder();

        // Map the mongodb object to a tuple
        MongoObjectGrabber mongoMapper = new MongoObjectGrabber() {
            @Override
            public List<Object> map(DBObject object) {
                List<Object> tuple = new ArrayList<Object>();
                // Add the op
                tuple.add(object.get("op").toString());
                // Add the id
                if (object.get("op").toString().equals("i") || object.get("op").toString().equals("d")) {
                    tuple.add(((BSONObject) object.get("o")).get("_id").toString());
                } else {
                    tuple.add(((BSONObject) object.get("o2")).get("_id").toString());
                }
                // Add the a variable
                tuple.add(((BSONObject) object.get("o")).get("x"));
                // Return the mapped object
                return tuple;
            }

            @Override
            public String[] fields() {
                return new String[]{"o", "_id", "x"};
            }
        };

        // The update query
        UpdateQueryCreator updateQuery = new UpdateQueryCreator() {
            @Override
            public DBObject createQuery(Tuple tuple) {
                return new BasicDBObject("aggregation_doc", "summary");
            }
        };

        // Field mapper
        StormMongoObjectGrabber mapper = new StormMongoObjectGrabber() {
            @Override
            public DBObject map(DBObject object, Tuple tuple) {
                return BasicDBObjectBuilder.start().push("$set").add("sum", tuple.getIntegerByField("sum")).get();
            }
        };

        // Create a mongo bolt
        MongoUpdateBolt mongoSaveBolt = new MongoUpdateBolt("mongodb://127.0.0.1:37017/test", "stormoutputcollection", updateQuery, mapper, WriteConcern.JOURNALED);
        // Set the spout
        builder.setSpout("mongodb", new MongoOpLogSpout("mongodb://127.0.0.1:37017", query, "test.aggregation", mongoMapper), 1);
        // Add a bolt
        builder.setBolt("sum", new SummarizerBolt(), 1).allGrouping("mongodb");
        builder.setBolt("mongo", mongoSaveBolt, 1).allGrouping("sum");

        // Set debug config
        Config conf = new Config();
        conf.setDebug(true);

        // Run on local cluster
        LocalCluster cluster = new LocalCluster();
        // Submit the topology

        //cluster.submitTopology("mongo-test", conf, builder.createTopology());
        // Starts inserts
        //latch.countDown();

        // Wait until we have the summation all done
        DBCollection collection = mongo.getDB("storm_mongospout_test").getCollection("stormoutputcollection");
        // Keep polling until it's done
        boolean done = false;

        // Keep checking until done
        while (!done) {
            DBObject result = collection.findOne(new BasicDBObject("aggregation_doc", "summary"));
            if (result != null && (Integer) result.get("sum") == 4950) {
                done = true;
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
        }

        // Kill the cluster
       /* cluster.killTopology("mongo-test");
        cluster.shutdown();*/
        // Close db
        StormSubmitter.submitTopologyWithProgressBar("mongo-test", conf, builder.createTopology());
        mongo.close();
        // Ensure we die
        System.exit(0);

    }


    // Bolt summarising numbers
    static class SummarizerBolt implements IBasicBolt {
        private int sum = 0;
        private int numberOfRecords = 0;

        public void prepare(Map map, TopologyContext topologyContext) {
        }

        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            // Add to sum
            this.sum = this.sum + tuple.getIntegerByField("a");
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
