package com.xwinie.calcite.adapter.json;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;

public class MainTestSchema {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Class.forName("org.apache.calcite.jdbc.Driver");

        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // 本地Schema.
        // Schema schema = ReflectiveSchema.create(calciteConnection, rootSchema, "hr",
        // new HrSchema());

        Class.forName("com.mysql.cj.jdbc.Driver");
        // 第一个数据库
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:mysql://192.168.138.233:33306");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        Schema schema = JdbcSchema.create(rootSchema, "assetbuymgr", dataSource, "assetbuymgr", null);
        rootSchema.add("assetbuymgr", schema);

        dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:mysql://192.168.138.235:33306");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
         schema = JdbcSchema.create(rootSchema, "asspmgr", dataSource, "asspmgr", null);
        rootSchema.add("asspmgr", schema);
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(
                "SELECT * FROM assetbuymgr.assetbuymgr_pay_comfirm_info as a inner join asspmgr.asspmgr_special_plan_basic_info  as c on  c.special_plan_periods=a.special_plan_periods ");

        output(resultSet, System.out);
        resultSet.close();
        statement.close();
        connection.close();
    }

    private static void output(ResultSet resultSet, PrintStream out) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1;; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }
}

