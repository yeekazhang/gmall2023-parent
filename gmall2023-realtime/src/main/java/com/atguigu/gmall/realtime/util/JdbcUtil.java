package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava30.com.google.common.base.CaseFormat;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    public static Connection getMysqlConnection() throws Exception {

        // 获取 jdbc连接
        // 1 加载驱动
        Class.forName(Constant.MYSQL_DRIVER);

        return DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USERNAME, Constant.MYSQL_PASSWORD);
    }

    /**
     * 执行一个查询语句，把查询到的结果封装到 T 类型的对象中
     * @param conn mysql连接
     * @param querySql 查询的 sql 语句
     * @param args sql 中的占位符的值
     * @param tClass T 类
     * @return 查询到的多行结果
     * @param <T> 每行封装的类型
     */
    public static <T> List<T> queryList(Connection conn,
                                        String querySql,
                                        Object[] args,
                                        Class<T> tClass, boolean ... isUnderlineToCamel) throws Exception {
        boolean defaultIsUToC = false; // 默认不转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];

        }

        List<T> result = new ArrayList<>();

        // select * from a where id=?
        // 1 通过连接对象获取一个预处理语句
        PreparedStatement statement = conn.prepareStatement(querySql);

        // 2 给占位符赋值
        for (int i = 0; args != null && i < args.length; i++) {
            Object v = args[i];
            statement.setObject(i + 1, v);
        }

        // 3 执行查询
        ResultSet resultSet = statement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();

        // 4 解析结果集，把数据封装到一个List集合中
        while (resultSet.next()) {
            // 变量到一行数据，把这个行数据封装到一个 T 类型的对象中
            T t = tClass.newInstance(); // 使用反射创建一个 T 类型的对象

            //遍历这一行的每一列数据
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                // 获取列名和列值
                String name = metaData.getColumnLabel(i);
                // 转小驼峰
                if (defaultIsUToC) {
                    name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
                }


                Object value = resultSet.getObject(i);
                // t.name=value
                BeanUtils.setProperty(t, name, value);
            }
            result.add(t);
        }

        return result;
    }

    public static void closeConnection(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()){
            conn.close();
        }
    }

    public static void main(String[] args) throws Exception {
        
        Connection conn = getMysqlConnection();

//        List<HashMap> list = queryList(conn, "select * from gmall2023.user_info", null, HashMap.class);
        List<TableProcess> list = queryList(conn, "select * from gmall2023_config.table_process", null, TableProcess.class, true);

        for (Object obj : list) {
            System.out.println(obj);
        }
    }
}
