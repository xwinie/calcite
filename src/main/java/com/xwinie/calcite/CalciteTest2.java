package com.xwinie.calcite;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import com.alibaba.fastjson.JSONObject;


public class CalciteTest2 {
    public static void main(String[] args) throws Exception {
        long begin = System.currentTimeMillis();
        new CalciteTest2().run();
        long duration = System.currentTimeMillis() - begin;
        System.out.println("total:" + duration);
    }

    public void run() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection optiqConnection = (CalciteConnection) connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = optiqConnection.getRootSchema();

        String json = "[{\"CUST_ID\":{\"a\":1},\"PROD_ID\":23.56,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
                + "{\"USER_ID\":310,\"CUST_ID\":{\"a\":2},\"PROD_ID\":210.45,\"USER_NAME\":\"user2\"},"
                + "{\"USER_ID\":320,\"CUST_ID\":{\"a\":3},\"PROD_ID\":210.46,\"USER_NAME\":\"user3\"},"
                + "{\"USER_ID\":330,\"CUST_ID\":{\"a\":4},\"PROD_ID\":210.47,\"USER_NAME\":\"user4\"},"
                + "{\"USER_ID\":340,\"CUST_ID\":{\"a\":5},\"PROD_ID\":210.48,\"USER_NAME\":\"user5\"},"
                + "{\"USER_ID\":350,\"CUST_ID\":{\"a\":6},\"PROD_ID\":210.49,\"USER_NAME\":\"user6\"},"
                + "{\"USER_ID\":360,\"CUST_ID\":{\"a\":7},\"PROD_ID\":210.40,\"USER_NAME\":\"user7\"}]";

        String json1 = "[{\"CUST_ID\":{\"a\":2},\"PROD_ID\":20.56,\"USER_ID\":666,\"USER_NAME\":\"user11\"},"
                + "{\"USER_ID\":310,\"CUST_ID\":{\"a\":2},\"PROD_ID\":210.45,\"USER_NAME\":\"user21\"},"
                + "{\"USER_ID\":320,\"CUST_ID\":{\"a\":3},\"PROD_ID\":210.46,\"USER_NAME\":\"user31\"},"
                + "{\"USER_ID\":330,\"CUST_ID\":{\"a\":4},\"PROD_ID\":210.47,\"USER_NAME\":\"user41\"},"
                + "{\"USER_ID\":310,\"CUST_ID\":{\"a\":5},\"PROD_ID\":210.48,\"USER_NAME\":\"user51\"},"
                + "{\"USER_ID\":310,\"CUST_ID\":{\"a\":6},\"PROD_ID\":210.49,\"USER_NAME\":\"user61\"},"
                + "{\"USER_ID\":360,\"CUST_ID\":{\"a\":7},\"PROD_ID\":210.40,\"USER_NAME\":\"user71\"}]";

        Statement statement = connection.createStatement();


        ResultSet resultSet = null;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 2; i++) {
            rootSchema.add("abc", new JsonSchema("test", json));
            rootSchema.add("abc1", new JsonSchema("test1", json1));
            resultSet = statement.executeQuery(
                    "select \"test1\".USER_NAME from \"abc" + "\".\"test\" inner  join  \"abc1" + "\".\"test1\"  on  \"test1\".USER_ID=\"test\".USER_ID  order by \"test\".USER_ID desc limit 2 offset 2 ");
        }
        System.out.println("query:" + (System.currentTimeMillis() - begin));

        while (resultSet.next()) {
            JSONObject jo = new JSONObject();
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= n; i++) {
                jo.put(resultSet.getMetaData().getColumnName(i), resultSet.getObject(i));
            }
            System.out.println(jo.toJSONString());
        }
        resultSet.close();
        statement.close();
        connection.close();
    }
}
