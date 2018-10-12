package demo;

import zzq.dolls.redis.RedisMode;
import zzq.dolls.redis.RedisPool;

import java.io.IOException;

public class JRedis {

    public static void main(String[] args) throws IOException {

        RedisPool pool = new RedisPool.Builder()
                .urls("10.144.58.44:7003;10.144.58.44:7004;10.144.58.44:7005;10.144.58.45:7000")
                .redisMode(RedisMode.CLUSTER)
                .build();

        //Set<String> set = pool.handler(j -> j.hkeys("test"));

        pool.cluster(c -> c.set("test", "22"));
        String s = pool.cluster(c -> c.get("test"));
        System.out.println(s);
        pool.close();
    }
}
