/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sparkhbaseloganalyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


/**
 *
 * @author rakib
 */
public class HBaseReader {

    public static void processLogRDD(JavaRDD<String> logRDD) {

        JavaRDD<EventLog> eventLogRDD = logRDD.map(new Function<String, EventLog>() {
            @Override
            public EventLog call(String logText) throws Exception {
                String[] logs = logText.split(" ");                        
                EventLog eventLog = new EventLog();
                eventLog.setRowKey(logs[0]);
                eventLog.setMethod(logs[3]);
                eventLog.setData(logs[2]);
                return eventLog;
            }
        });
        System.out.println(eventLogRDD.count());
        processRDD(eventLogRDD);
    }

    public static void main(String[] args) throws IOException {

        HTable table = HBaseManager.getHBaseManager().createHTable("2017100811_tmp");
        ResultScanner scanner = table.getScanner(new Scan());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("HBaseReader").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<String> logString = new ArrayList<>();
        String log = null;

        String schemaString = "rowKey method";

        for (Result rowResult : scanner) {

            log = "";
            String rowKey = Bytes.toString(rowResult.getRow());
            //System.out.println(rowKey);
            for (byte[] columeFamily : rowResult.getMap().keySet()) {

                String data_method = "";
//                String colFamiley = Bytes.toString(columeFamily);
//                System.out.println("row Key:"+rowKey + " Colume Famaily:"+colFamiley);
                for (byte[] cols : rowResult.getMap().get(columeFamily).keySet()) {
//                    String colName = Bytes.toString(cols);
//                    System.out.println("row key:"+ rowKey +"Colume Family:"+colFamiley+" Colume Name:"+ colName);
                    for (long value : rowResult.getMap().get(columeFamily).get(cols).keySet()) {
                        String value_string = new String(rowResult.getMap().get(columeFamily).get(cols).get(value));
                        data_method += " " + value_string;

                    }
                }
                log += rowKey + " " + data_method;
            }
            logString.add(log);

        }
        // Generate the schema based on the string of schema

        JavaRDD<String> logRDD = javaSparkContext.parallelize(logString);

        processLogRDD(logRDD);

        //logRDD.foreach(p->System.out.println(p));
    }

    private static void processRDD(JavaRDD<EventLog> rdd) {
        rdd.foreach(p->{
            System.out.println("Time: "+p.getRowKey()+" Method: "+p.getMethod());
        });
        
    }
}
