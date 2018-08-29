package demo

import cn.ac.iie.dolls.db.JDBCPool

fun main(args: Array<String>) {

    val config = hashMapOf(
            "url" to "jdbc:mysql://localhost:3306/mydb?useSSL=false",
            "username" to "iie",
            "password" to "iie"
    )

    val mysql = JDBCPool(config).init()

    with(mysql) {
        val select = select("select 1") { rs ->
            val list = arrayListOf<String>()
            while (rs.next()) {
                list.add(rs.getString(1))
            }
            return@select list
        }
        select.forEach(::println)

        insert("sql")
        delete("sql")
    }
}