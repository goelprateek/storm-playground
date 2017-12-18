package com.prateek.storm.nextgen.spouts;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MongoReader extends BaseRichSpout{

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoReader.class);

    private SpoutOutputCollector spoutOutputCollector;

    private final String mongoDBUrl;

    private final String collectionName;

    private MongoCollection<Document> collection;

    private AtomicLong atomicLong;

    public MongoReader(String mongoDBUrl, String collectionName) {
        this.mongoDBUrl = mongoDBUrl;
        this.collectionName = collectionName;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.spoutOutputCollector = collector;

        MongoClientURI mongoClientURI = new MongoClientURI(mongoDBUrl);

        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoDBUrl));

        MongoDatabase database = mongoClient.getDatabase(mongoClientURI.getDatabase());

        collection = database.getCollection(collectionName);


    }

    @Override
    public void nextTuple() {
        atomicLong = new AtomicLong(0);
        if(null != collection){

            FindIterable<Document> documents = collection.find();
            LOGGER.debug(" preparing to emmit tuple from mongo collection");
            if(null != documents){
                for(Document doc: documents){
                    LOGGER.debug("document fetched from mongo [{}]", doc);
                    atomicLong.incrementAndGet();
                    spoutOutputCollector.emit("MongoReader",new Values(doc.get("x"), doc.get("y")));
                }
            }else{
                LOGGER.debug(" No document found in specified collection [{}] ", this.collectionName);
            }

            LOGGER.debug(" total number of document emitted are [{}]" , atomicLong.get());

        }else{
            LOGGER.error("no collection named [{}] found in mongodb.. Please check and supply valid collection ", this.collectionName);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("x","y"));
    }
}
