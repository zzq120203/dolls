# dolls
小工具 -- iiemq(基于加载1.5.8版jar包),redis连接池,基于druid的数据库连接池(druid不识别mpp链接地址，可调用。。，或修改druid部分源码)

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
或

    consumer.message(Data::new, data -> {
        ......
    });
启动
    
    consumer.start();
关闭consumer

    consumer.stop();
关闭连接
    
    client.close();

redis：

获取连接池

    RedisPool pool = RedisPool.builder().urls(url).build();
或

    RedisPool pool = RedisPool.builder().urls(url).redisMode(RedisMode.STANDALONE).build();
操作redis

STANDALONE、SENTINEL模式
    
    pool.jedis(jedis -> jedis.keys("*"));
CLUSTER模式
    
    pool.cluster(jedis -> jedis.keys("*"));
关闭连接池：

    pool.close();

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



--------------------------
注：接口内部实现了释放连接，无需调用
