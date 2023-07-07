package com.atguigu.test;

import com.atguigu.gmall.realtime.util.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;

public class NormalTest {

    private Connection conn;

    @Test
    public void testConn() throws IOException {
        conn = HBaseUtil.getHBaseConnection();
        System.out.println(conn);

        TableName tableName = TableName.valueOf("gmall", "trade_mark");
        System.out.println("tableName:" + tableName);
        Table table = conn.getTable(tableName);
        if(table != null){
            System.out.println("table:" + table);
        } else {
            System.out.println("不存在");
        }
    }
}
