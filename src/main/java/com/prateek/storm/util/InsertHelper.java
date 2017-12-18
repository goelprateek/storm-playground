package com.prateek.storm.util;

import com.mongodb.*;

import java.util.concurrent.CountDownLatch;

public class InsertHelper implements Runnable {
    private String dbName;
    private String collectionName;
    private CountDownLatch latch;

    public InsertHelper(String dbName, String collectionName, CountDownLatch latch) {
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.latch = latch;
    }

    public void run() {
        // Initialize the mongo object
        Mongo mongo;
        try {
            // Open connection
            mongo = new MongoClient("localhost", 27017);
            // Fetch the local db
            DB db = mongo.getDB(dbName);
            // Holds our collection for the oplog
            DBCollection collection = db.getCollection(collectionName);
            // Now insert a bunch of docs once we are ready
            while (latch.getCount() != 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // Insert one hundred objects
            for (int i = 0; i < 100; i++) {
                // Create a basic object
                BasicDBObject object = new BasicDBObject();
                object.put("a", i);
                // Insert the object
                collection.insert(object, WriteConcern.SAFE);
            }

            mongo.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
