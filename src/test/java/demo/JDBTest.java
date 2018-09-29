package demo;

import cn.ac.iie.dolls.db.JDBCPool;
import cn.ac.iie.dolls.redis.RPoolProxy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBTest {

    public static void main(String[] args) throws SQLException {

        Map<String, String> config = new HashMap<>();
        config.put("url", "jdbc:mysql://localhost:3306/mydb?useSSL=false");
        config.put("username", "iie");
        config.put("password", "iie");

        JDBCPool mysql = new JDBCPool(config);

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


        RPoolProxy rpp = new RPoolProxy(null);

    }
}
