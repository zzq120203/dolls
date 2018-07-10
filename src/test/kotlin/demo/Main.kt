package demo

import cn.ac.iie.dolls.JDBCPool

fun main(args: Array<String>) {

    val config = hashMapOf(
            "url" to "jdbc:mysql://localhost:3306/mydb?useSSL=false",
            "username" to "iie",
            "password" to "iie",
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

    val mysql = JDBCPool(config).init()

    val select = mysql.select("select 1") { rs ->
        val list = arrayListOf<String>()
        while (rs.next()) {
            list.add(rs.getString(1))
        }
        return@select list
    }
    select.forEach(::println)

    mysql.insert("sql")
    mysql.delete("sql")
}