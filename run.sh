#!/usr/bin/env bash


echo $STORM_HOME

storm jar target/storm-demo.jar com.prateek.storm.nextgen.topologies.KafkaTopology