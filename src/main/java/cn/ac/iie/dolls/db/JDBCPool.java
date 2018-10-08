package cn.ac.iie.dolls.db;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class JDBCPool {

    private Map<String, String> config;

    private DataSource dataSource;

    /**
     * 创建连接池
     * @param config 参数列表，可通过Builder定制创建
     */
    public JDBCPool(Map<String, String> config) {
        initConfig();
        this.config.putAll(config);
        try {
            dataSource = DruidDataSourceFactory.createDataSource(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置连接默认值
     */
    private void initConfig() {
        this.config = new HashMap<>();
        this.config.put("url", "jdbc:mysql://localhost:3306/mydb?useSSL=false");
        this.config.put("username", "zzq");
        this.config.put("password", "zzq");
        this.config.put("filters", "stat");
        this.config.put("initialSize", "5");
        this.config.put("maxActive", "300");
        this.config.put("maxWait", "60000");
        this.config.put("timeBetweenEvictionRunsMillis", "60000");
        this.config.put("minEvictableIdleTimeMillis", "300000");
        this.config.put("validationQuery", "SELECT 1");
        this.config.put("testWhileIdle", "true");
        this.config.put("testOnBorrow", "false");
        this.config.put("testOnReturn", "false");
        this.config.put("poolPreparedStatements", "true");
        this.config.put("maxPoolPreparedStatementPerConnectionSize", "200");
    }

    /**
     * 创建连接池
     * @param builder 通过Builder定制创建
     */
    private JDBCPool(Builder builder) {
        initConfig();
        this.config.putAll(builder.config);
        try {
            dataSource = DruidDataSourceFactory.createDataSource(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final class Builder {
        private Map<String, String> config = new HashMap<>();

        public JDBCPool build() {
            return new JDBCPool(this);
        }

        public Builder url(String url) {
            config.put("url", url);
            return this;
        }

        public Builder userName(String userName) {
            config.put("username", userName);
            return this;
        }

        public Builder password(String password) {
            config.put("password", password);
            return this;
        }

        /**
         *
         * @param filters 默认 SELECT 1
         * @return
         */
        public Builder filters(String filters) {
            config.put("filters", filters);
            return this;
        }

        /**
         *
         * @param initialSize 默认 5
         * @return
         */
        public Builder initialSize(int initialSize) {
            config.put("url", String.valueOf(initialSize));
            return this;
        }

        /**
         *
         * @param maxActive 默认 300
         * @return
         */
        public Builder maxActive(int maxActive) {
            config.put("url", String.valueOf(maxActive));
            return this;
        }

        /**
         *
         * @param maxWait 默认 60000
         * @return
         */
        public Builder maxWait(int maxWait) {
            config.put("url", String.valueOf(maxWait));
            return this;
        }

        /**
         *
         * @param timeBetweenEvictionRunsMillis 默认 60000
         * @return
         */
        public Builder timeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
            config.put("url", String.valueOf(timeBetweenEvictionRunsMillis));
            return this;
        }

        /**
         *
         * @param minEvictableIdleTimeMillis 默认 300000
         * @return
         */
        public Builder minEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
            config.put("url", String.valueOf(minEvictableIdleTimeMillis));
            return this;
        }

        /**
         *
         * @param validationQuery 默认 stat
         * @return
         */
        public Builder uvalidationQueryrl(String validationQuery) {
            config.put("url", validationQuery);
            return this;
        }

        /**
         *
         * @param testWhileIdle 默认 true
         * @return
         */
        public Builder testWhileIdle(boolean testWhileIdle) {
            config.put("url", String.valueOf(testWhileIdle));
            return this;
        }

        /**
         *
         * @param testOnBorrow 默认 false
         * @return
         */
        public Builder testOnBorrow(boolean testOnBorrow) {
            config.put("url", String.valueOf(testOnBorrow));
            return this;
        }

        /**
         *
         * @param testOnReturn 默认 false
         * @return
         */
        public Builder testOnReturn(boolean testOnReturn) {
            config.put("url", String.valueOf(testOnReturn));
            return this;
        }

        /**
         *
         * @param poolPreparedStatements 默认 true
         * @return
         */
        public Builder poolPreparedStatements(boolean poolPreparedStatements) {
            config.put("url", String.valueOf(poolPreparedStatements));
            return this;
        }

        /**
         *
         * @param maxPoolPreparedStatementPerConnectionSize 默认 200
         * @return
         */
        public Builder maxPoolPreparedStatementPerConnectionSize(int maxPoolPreparedStatementPerConnectionSize) {
            config.put("url", String.valueOf(maxPoolPreparedStatementPerConnectionSize));
            return this;
        }
    }

    /**
     * 查询操作
     * @param sql sql语句
     * @param fun ResultSet 解析方法
     * @param <T> 返回数据
     * @return T
     * @throws SQLException 默认sql异常
     */
    public <T> T select(String sql, Function<ResultSet, T> fun) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)
        ) {
            return fun.apply(resultSet);
        }
    }

    /**
     * 更新操作
     * @param sql sql语句
     * @return 返回成功条数
     * @throws SQLException 默认sql异常
     */
    public int update(String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            return statement.executeUpdate(sql);
        }
    }

    /**
     * 插入操作
     * @param sql sql语句
     * @return 返回成功条数
     * @throws SQLException 默认sql异常
     */
    public int insert(String sql) throws SQLException {
        return update(sql);
    }

    /**
     * 删除操作
     * @param sql sql语句
     * @return 返回成功条数
     * @throws SQLException 默认sql异常
     */
    public int delete(String sql) throws SQLException {
        return update(sql);
    }

}