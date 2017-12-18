package com.prateek.storm.core;

import com.mongodb.DBObject;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;

public abstract class UpdateQueryCreator implements Serializable {

  public abstract DBObject createQuery(Tuple tuple);

}
