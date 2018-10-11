package demo;

import cn.ac.iie.dolls.redis.RedisMode;
import cn.ac.iie.dolls.redis.RedisPool;

import java.util.Set;

public class JRedis {

    public static void main(String[] args) {

        RedisPool pool = new RedisPool.Builder()
                .urls("localhost:8080")
                .authToken("zzq")
                .redisMode(RedisMode.STANDALONE)
                .maxTotal(8)
                .maxIdle(8)
                .timeout(60 * 1000)
                .masterName("nomaster")
                .build();

        Set<String> set = pool.handler(j -> j.hkeys("test"));

        set = pool.cluster(c -> c.hkeys("test"));
    }
}
