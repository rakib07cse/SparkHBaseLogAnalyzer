/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sparkhbaseloganalyzer;


import utils.Tools;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteProtocol;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author rakib
 */
public class HBaseManager {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final static String ZK_QUORUM = "localhost";
    private final static int ZK_CLIENT_PORT = 2181;

    private static Configuration hBaseConfig = null;
    private static HBaseAdmin hBaseAdmin = null;

    private static class HBaseManagerSingleton {

        private final static HBaseManager HBASE_MANAGER_INSTANCE = new HBaseManager();
    }

    public static HBaseManager getHBaseManager() {
        return HBaseManagerSingleton.HBASE_MANAGER_INSTANCE;
    }

    private HBaseManager() {
        if (HBaseManager.HBaseManagerSingleton.HBASE_MANAGER_INSTANCE != null) {
            throw new InstantiationError("Creating of this object is not allowed. The singleton object is accessible by HBaseManager.getHBaseManager()");
        }
    }

    public Configuration getConfiguration() {
        if (hBaseConfig == null) {
            hBaseConfig = HBaseConfiguration.create();
            hBaseConfig.set(HConstants.ZOOKEEPER_QUORUM, ZK_QUORUM);
            hBaseConfig.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, ZK_CLIENT_PORT);
        }
        return hBaseConfig;
    }

    /**
     * Return connection instance from Connection pool Handy for high-end
     * multi-threaded access
     *
     * @return HConnection instance
     */
    public HConnection createHConnection() {
        try {
            HConnection hConnection = HConnectionManager.createConnection(getConfiguration());
            return hConnection;
        } catch (ZooKeeperConnectionException ex) {
            logger.error(ex.toString());
        }
        return null;
    }

    public HBaseAdmin getAdmin() {
        if (hBaseAdmin == null) {
            try {
                hBaseAdmin = new HBaseAdmin(getConfiguration());
                return hBaseAdmin;
            } catch (MasterNotRunningException | ZooKeeperConnectionException ex) {
                logger.error(ex.toString());
            }
        }
        return hBaseAdmin;
    }

    /**
     * Create HBase Table instance
     *
     * @param tableName
     * @throws java.io.IOException
     */
    public void createHBaseTable(String tableName) throws IOException {

        HBaseAdmin hbaseAdmin = getHBaseManager().getAdmin();
        if (!hbaseAdmin.tableExists(tableName)) {
            //Instantiating table descriptor class
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            //Adding Colume table families to table descriptor
            tableDescriptor.addFamily(new HColumnDescriptor(Tools.HBASE_TABLE_COLUME_FAMILY_NAME));

            //Execute the table Through admin
            hBaseAdmin.createTable(tableDescriptor);
//            String msg = "Table '" + tableName + "' successfully created.";
//            throw new IOException(msg);
        } else {
            String msg = "Table '" + tableName + "' table already exist.";
            throw new IOException(msg);
        }
    }

    /**
     * Create HTable instance. HTable is not thread-safe, not suitable for
     * multi-threaded scenario Must invoke close() after operation
     *
     * @param tableName
     * @return HTable instance
     * @throws IOException
     */
    public HTable createHTable(String tableName) throws IOException {
        HBaseAdmin hBaseAdmin = getHBaseManager().getAdmin();
        if (!hBaseAdmin.tableExists(tableName)) {
            String msg = "Table '" + tableName + "' doesn't exist in hbase";
//            logger.error(msg);
            throw new IOException(msg);
        }
        if (hBaseAdmin.isTableDisabled(tableName)) {
            String msg = "Table '" + tableName + "' is disabled";
//            logger.error(msg);
            throw new IOException(msg);
        }
        return new HTable(getHBaseManager().getConfiguration(), tableName);
    }

    /**
     * create thread safe table from HConnection pool
     *
     * @param connection
     * @param tableName
     * @return
     * @throws java.io.IOException
     */
    public HTableInterface getHTable(HConnection connection, String tableName) throws IOException {
        if (getAdmin().tableExists(tableName)) {
            String msg = "Table '" + tableName + "' doesn't exist in hbase";
//            logger.error(msg);
            throw new IOException(msg);
        }
        if (getAdmin().isTableDisabled(tableName)) {
            String msg = "Table '" + tableName + "' is disabled";
//            logger.error(msg);
            throw new IOException(msg);
        }
        return connection.getTable(tableName);
    }

    /**
     * Create Put Instance
     *
     * @param logid
     * @return
     */
    public Put createLoggerPut(String logid) {

        return new Put(Bytes.toBytes(logid));
    }

    public long bulkDelete(String tableName, Map<String, String> columnMap, int batchSize) throws IOException {
//        HConnection connection = createHConnection();
//        HTableInterface table = getTable(connection, tableName);
        HTable table = createHTable(tableName);

        long noOfDeletedRows = 0L;
        Scan scan = new Scan();
        ResultScanner rowScanner = table.getScanner(scan);
        List<Delete> deleteBatch = new ArrayList<Delete>();

        for (Result row : rowScanner) {
            Delete delete = new Delete(row.getRow());
            for (Entry<String, String> column : columnMap.entrySet()) {
                delete.deleteColumns(column.getKey().getBytes(), column.getValue().getBytes());
            }
            deleteBatch.add(delete);

            if (deleteBatch.size() >= batchSize) {
                noOfDeletedRows += batchSize;
                table.delete(deleteBatch);
                logger.info("Rows " + (noOfDeletedRows - batchSize + 1) + " to " + noOfDeletedRows + " processed.");
            }
        }
        if (!deleteBatch.isEmpty()) {
            int deleteCount = deleteBatch.size();
            noOfDeletedRows += deleteCount;
            table.delete(deleteBatch);
            logger.info("Rows " + ((deleteCount > 1) ? (noOfDeletedRows - deleteCount + 1) + " to " + noOfDeletedRows : noOfDeletedRows) + " processed.");

        }
        table.close();
//        connection.close();

        return noOfDeletedRows;
    }

    /**
     * from hbase-0.94.27 and onwards
     */
    public long performBulkDeletion(byte[] tableName, final Scan scan, final int rowBatchSize,
            final byte deleteType, final Long timeStamp) throws Throwable {

        HConnection connection = createHConnection();
        HTableInterface table = connection.getTable(tableName);

        long noOfDeletedRows = 0L;
        Batch.Call<BulkDeleteProtocol, BulkDeleteResponse> callable = new Batch.Call<BulkDeleteProtocol, BulkDeleteResponse>() {
            public BulkDeleteResponse call(BulkDeleteProtocol instance) throws IOException {
                return instance.delete(scan, deleteType, timeStamp, rowBatchSize);
            }
        };
        Map<byte[], BulkDeleteResponse> result = table.coprocessorExec(BulkDeleteProtocol.class, scan.getStartRow(),
                scan.getStopRow(), callable);
        for (BulkDeleteResponse response : result.values()) {
            noOfDeletedRows += response.getRowsDeleted();
        }

        table.close();
        connection.close();

        return noOfDeletedRows;
    }

    public void close() throws IOException {
        if (hBaseAdmin != null) {
            hBaseAdmin.close();
        }
    }

}
