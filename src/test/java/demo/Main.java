package demo;

import cn.ac.iie.dolls.JDBCPool;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws SQLException {

        Map<String, String> config = new HashMap<>();
        config.put("url", "jdbc:mysql://localhost:3306/mydb?useSSL=false");
        config.put("username", "iie");
        config.put("password", "iie");
        config.put("filters", "stat");
        config.put("initialSize", "5");
        config.put("maxActive", "300");
        config.put("maxWait", "60000");
        config.put("timeBetweenEvictionRunsMillis", "60000");
        config.put("minEvictableIdleTimeMillis", "300000");
        config.put("validationQuery", "SELECT 1");
        config.put("testWhileIdle", "true");
        config.put("testOnBorrow", "false");
        config.put("testOnReturn", "false");
        config.put("poolPreparedStatements", "true");
        config.put("maxPoolPreparedStatementPerConnectionSize", "200");

        JDBCPool mysql = new JDBCPool(config).init();

        List<String> select = mysql.select("select 1", rs -> {
            List<String> list = new ArrayList<>();
            try {
                while (rs.next()) {
                    list.add(rs.getString(1));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return list;
        });
        select.forEach(System.out::println);

        mysql.insert("sql");
        mysql.delete("sql");
    }
}
