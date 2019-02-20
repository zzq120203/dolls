# dolls
小工具(iiemq,redis连接池,基于druid的数据库连接池)  
环境 jdk8+

mq:

获取连接

    IIEClient client = IIEClient.builder().url(url).build();
创建consumer

    IIEConsumer consumer = client.newConsumer().group(group).topic(topic).build();
获取消息
    
    consumer.message(record -> {
        try {
            String key = record.getString("g_ch_key");
            List<String> list = record.getStrings("m_video_files");

        } catch (RESessionException e) {
            log.error(e.getMessage(), e);
        }
        return true;
    });

redis：

获取连接池（STANDALONE模式）
    
    RedisPool pool = RedisPool.builder().urls(url).redisMode(RedisMode.STANDALONE).build();
操作redis
    
    pool.jedis(jedis -> jedis.keys("*"));


db:

获取连接池

    JDBCPool pool = JDBCPool.builder().url(url).userName(user).password(passwd).build();
查询

    pool.select("select 1", result -> {
        while (result.next()) {
            result.getString(1);
        }
    });
更新

    pool.update("update ....");


