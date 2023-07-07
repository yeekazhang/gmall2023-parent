package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop162");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        return ConnectionFactory.createConnection(conf);

    }

    public static void closeHBaseConn(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    public static void createHBaseTable(Connection hbaseConn,
                                        String namespace,
                                        String table,
                                        String family) throws IOException {
        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(namespace, table);

        // 判断要建的表是否存在
        if (admin.tableExists(tableName)) {
            return;
        }

        // 列族描述器
        ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.of(family);

        // 表的描述器
        TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(cfDesc)
                .build();
        admin.createTable(desc);
        admin.close();
        System.out.println(namespace + ":" + table + " 建表成功");
    }

    public static void dropTable(Connection hbaseConn, String namespace, String table) throws IOException {

        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(namespace, table);

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        admin.close();
        System.out.println(namespace + ":" + table + " 删除成功");
    }

    public static void putRow(Connection conn,
                              String namespace,
                              String hbaseTable,
                              String rowKey,
                              String family,
                              JSONObject data) throws IOException {
        // 1 获取table对象
        TableName tableName = TableName.valueOf(namespace, hbaseTable);
        Table table = conn.getTable(tableName);
        // 2 创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        // 3 把每列放入put对象中
        for (String key : data.keySet()) {
            String value = data.getString(key);
            if (value != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value));
            }
        }

        // 4 向 table 对象中 put 数据
        table.put(put);
        table.close();
    }

    public static void delRow(Connection conn, String namespace, String sinkTable, String rowKey) throws IOException {

        TableName tableName = TableName.valueOf(namespace, sinkTable);
        Table table = conn.getTable(tableName);

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除整行
        table.delete(delete);

        table.close();
    }
}








