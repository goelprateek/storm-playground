package com.prateek.storm.nextgen.spouts;

import com.opencsv.CSVReader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CsvSpout extends BaseRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvSpout.class);

    private SpoutOutputCollector spoutOutputCollector;

    private String fileName;

    private final char separator;

    private CSVReader csvReader;

    private boolean includeHeaderRow;

    private AtomicLong linesRead;


    public CsvSpout(String fileName, char separator, boolean includeHeaderRow) {
        this.fileName = fileName;
        this.separator = separator;
        this.includeHeaderRow = includeHeaderRow;
        this.linesRead = new AtomicLong(0);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.spoutOutputCollector = collector;

        try {

            csvReader = new CSVReader(new FileReader(fileName), separator);

            if (includeHeaderRow) csvReader.readNext();

        } catch (Exception e) {

        }
    }

    @Override
    public void nextTuple() {

        try {

            String[] lines = csvReader.readNext();
            long counter = linesRead.incrementAndGet();
            if (null != lines) {
                spoutOutputCollector.emit(new Values(lines), counter);
            } else {
                LOGGER.info(" finishing line read , total lines read count is [{}]", linesRead.get());
            }

        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error(" exception occurs while reading line with probable cause [{}]", e.getMessage());
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        try {


            CSVReader reader = new CSVReader(new FileReader(fileName), separator);

            String[] strings = reader.readNext();

            if (includeHeaderRow) {
                // get column name and declare them

                declarer.declare(new Fields(Arrays.asList(strings)));


            } else {
                // fallback to column index
                ArrayList<String> objects = new ArrayList<>(strings.length);
                for (int i = 0; i < strings.length; i++) {
                    objects.add(strings[i]);

                }
                declarer.declare(new Fields(objects));
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
