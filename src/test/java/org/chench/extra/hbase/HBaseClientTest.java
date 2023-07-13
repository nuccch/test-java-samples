package org.chench.extra.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * 使用HBase客户端操作表
 * @author chench
 * @desc org.chench.extra.hbase.HBaseClientTest
 * @date 2022.11.01
 */
public class HBaseClientTest {
    String hbaseZkQuorum = "192.168.56.104";
    String hbaseZkPClientPort = "2182";
    String hbaseZkZnodeParent = "";

    private static String TABLE = "APP:article_realtime";
    private static String CF = "f";
    private static String QUALIFIER = "c";
    private static String VALUE = "1";

    @Test
    public void testQuery() throws IOException {
        Set<String> rowKeys = Collections.EMPTY_SET;
        try (HBaseClient client = new HBaseClient(hbaseZkQuorum, hbaseZkPClientPort, hbaseZkZnodeParent)) {
            for (String rowKey : rowKeys) {
                String value = client.getCell(TableName.valueOf(TABLE), rowKey, CF, QUALIFIER);
                System.out.println(String.format("rowkey: %s, value: %s", rowKey, value));
            }
        }
    }

    @Test
    public void testCount() throws IOException {
        long count = 0;
        try (HBaseClient client = new HBaseClient(hbaseZkQuorum, hbaseZkPClientPort, hbaseZkZnodeParent)) {
            count = client.rowCountByCoprocessor(TABLE);
        }
        Assert.assertTrue(count >= 0);
    }

    @Test
    public void testAddRows() throws IOException{
        List<Put> puts = new ArrayList<>();
        String rowKey = String.valueOf(System.currentTimeMillis());
        Put put = new Put(Bytes.toBytes(rowKey));
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(Bytes.toBytes(CF), Bytes.toBytes(QUALIFIER), Bytes.toBytes(VALUE));
        puts.add(put);
        boolean batch = false;
        try (HBaseClient client = new HBaseClient(hbaseZkQuorum, hbaseZkPClientPort, hbaseZkZnodeParent)) {
            client.addRows(TABLE, puts, batch);
        }
    }
}
