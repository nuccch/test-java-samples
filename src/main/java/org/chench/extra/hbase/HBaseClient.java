package org.chench.extra.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * HBase client
 *
 * https://hbase.apache.org/book.html#architecture.client
 *
 * @author chench
 * @desc org.chench.extra.hbase.HBaseClient
 * @date 2022.11.01
 */
@Slf4j()
public class HBaseClient implements Closeable {
    private Connection connection = null;
    private Configuration AggregationClientConf = null;

    public HBaseClient(String hbaseZkQuorum, String hbaseZkPClientPort, String hbaseZkZnodeParent) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", hbaseZkPClientPort);
        if (StringUtils.isNotEmpty(hbaseZkZnodeParent) && hbaseZkZnodeParent.trim().length() > 0) {
            conf.set("zookeeper.znode.parent", hbaseZkZnodeParent);
        }
        //conf.set("hadoop.user.name", "chenchanghui");
        this.AggregationClientConf = conf;
        log.info("hbase.zookeeper.quorum: {}", hbaseZkQuorum);
        log.info("hbase.zookeeper.property.clientPort: {}", hbaseZkPClientPort);
        log.info("zookeeper.znode.parent: {}", hbaseZkZnodeParent);
        this.connection = createConnection(conf);
    }

    /**
     * 批量添加行
     *
     * @param tableName
     * @param puts
     * @param batch
     * @throws IOException
     */
    public void addRows(String tableName, List<Put> puts, boolean batch) throws IOException {
        if (puts == null || puts.isEmpty()) {
            return;
        }
        log.info("Begin puts {} rows.", puts.size());
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            log.info("Got table.");
            long start = System.currentTimeMillis();
            if (batch) {
                table.batch(puts, new Object[puts.size()]);
            } else {
                table.put(puts);
            }
            log.info("Puts {} rows, cost {} ms.", puts.size(), System.currentTimeMillis() - start);
            puts.clear();
            log.info("Reset.");
        } catch (InterruptedException e) {
            log.error("InterruptedException " + e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    /**
     * 根据row key、column 读取
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @throws IOException
     */
    public String getCell(TableName tableName, String rowKey, String columnFamily, String column) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

            Result result = table.get(get);
            List<Cell> cells = result.listCells();

            if (CollectionUtils.isEmpty(cells)) {
                return null;
            }
            String value = new String(CellUtil.cloneValue(cells.get(0)), "UTF-8");
            return value;
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * 统计Hbase表总行数
     *
     * @param tablename
     */
    public long rowCountByCoprocessor(String tablename){
        try {
            //提前创建connection和conf
            Admin admin = connection.getAdmin();
            TableName name= TableName.valueOf(tablename);
            //先disable表，添加协处理器后再enable表
            admin.disableTable(name);
            HTableDescriptor descriptor = admin.getTableDescriptor(name);
            String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
            if (! descriptor.hasCoprocessor(coprocessorClass)) {
                descriptor.addCoprocessor(coprocessorClass);
            }
            admin.modifyTable(name, descriptor);
            admin.enableTable(name);

            //计时
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            Scan scan = new Scan();
            AggregationClient aggregationClient = new AggregationClient(this.AggregationClientConf);

            long count = aggregationClient.rowCount(name, new LongColumnInterpreter(), scan);
            System.out.println("RowCount: " + count);
            stopWatch.stop();
            System.out.println("统计耗时：" +stopWatch.getTime());
            aggregationClient.close();
            return count;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public void close() {
        closeConnection();
    }

    private Connection createConnection(Configuration conf) throws IOException {
        closeConnection();
        return ConnectionFactory.createConnection(conf);
    }

    private void closeConnection() {
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}