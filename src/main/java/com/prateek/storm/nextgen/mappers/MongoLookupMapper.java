package com.prateek.storm.nextgen.mappers;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import javax.swing.text.Document;
import java.io.Serializable;
import java.util.List;

public interface MongoLookupMapper extends Serializable{

    List<Values> toTuple(ITuple input, Document doc);

    void declareOutputFields(OutputFieldsDeclarer declarer);

}
