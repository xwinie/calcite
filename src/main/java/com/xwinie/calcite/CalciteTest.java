package com.xwinie.calcite;

import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class CalciteTest {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection optiqConnection = (CalciteConnection) connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = optiqConnection.getRootSchema();
        ReflectiveSchema hrs = new ReflectiveSchema(new JavaHrSchema());
        rootSchema.add("hr", hrs);
        Statement statement = optiqConnection.createStatement();

        ResultSet resultSet = statement.executeQuery(
                "select * from hr.emps as e join hr.depts as d on e.deptno = d.deptno");

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

