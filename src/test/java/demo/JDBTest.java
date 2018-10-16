package demo;

import zzq.dolls.db.JDBCPool;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBTest {

    public static void main(String[] args) throws SQLException {

        JDBCPool mysql = new JDBCPool.Builder()
                .url("jdbc:mysql://localhost:3306/mydb?useSSL=false")
                .userName("iie")
                .password("iie")
                .build();

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
