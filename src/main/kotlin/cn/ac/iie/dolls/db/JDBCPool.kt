package cn.ac.iie.dolls.db

import com.alibaba.druid.pool.DruidDataSourceFactory
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import javax.sql.DataSource

/**
 * 采用阿里druid的连接池
 */
class JDBCPool(private val config: Map<String, String>) {

    private val conf: MutableMap<String, String> by lazy {
        hashMapOf(
                "url" to "jdbc:mysql://localhost:3306/mydb?useSSL=false",
                "username" to "zzq",
                "password" to "zzq",
                "filters" to "stat",
                "initialSize" to "5",
                "maxActive" to "300",
                "maxWait" to "60000",
                "timeBetweenEvictionRunsMillis" to "60000",
                "minEvictableIdleTimeMillis" to "300000",
                "validationQuery" to "SELECT 1",
                "testWhileIdle" to "true",
                "testOnBorrow" to "false",
                "testOnReturn" to "false",
                "poolPreparedStatements" to "true",
                "maxPoolPreparedStatementPerConnectionSize" to "200"
        )
    }
    fun init(): JDBCPool {
        conf.putAll(config)
        return this
    }

    private val pool: DataSource by lazy {
        DruidDataSourceFactory.createDataSource(conf)
    }

    @Throws(SQLException::class)
    fun <T> select(sql: String, resultFun: (rs: ResultSet) -> T): T {
        var connection: Connection? = null
        var statement: Statement? = null
        var resultSet: ResultSet? = null

        try {
            connection = pool.connection
            statement = connection.createStatement()
            resultSet = statement.executeQuery(sql)
            return resultFun(resultSet)
        } finally {
            resultSet?.close()
            statement?.close()
            connection?.close()
        }
    }

    @Throws(SQLException::class)
    fun update(sql: String): Int {

        var connection: Connection? = null
        var statement: Statement? = null

        try {
            connection = pool.connection
            statement = connection.createStatement()
            return statement.executeUpdate(sql)
        } finally {
            statement?.close()
            connection?.close()
        }
    }

    @Throws(SQLException::class)
    fun insert(sql: String): Int = update(sql)

    @Throws(SQLException::class)
    fun delete(sql: String): Int = update(sql)
}