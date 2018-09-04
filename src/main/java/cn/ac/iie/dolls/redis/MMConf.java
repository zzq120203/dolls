package cn.ac.iie.dolls.redis;

import redis.clients.jedis.HostAndPort;

import java.util.Set;

/**
 * 多媒体配置
 */
public class MMConf {
    /**
     * STANDALONE, SENTINEL, CLUSTER,
     */
    public enum RedisMode {
        STANDALONE, SENTINEL, CLUSTER,
    }

    private String urls = null;

    // AuthPass
    private String authToken = null;

    // for standalone mode
    private HostAndPort hap;

    // for sentinel mode
    private Set<String> sentinels;

    // redis mode: sentinel or standalone
    private RedisMode redisMode;

    // redis connect timeout in 30s
    private int redisTimeout = 30 * 1000;

    // should RPS use cache?
    private boolean rpsUseCache = false;

    private int maxTotal = 8;
    private int maxIdle = 8;

    public String getUrls() {
        return urls;
    }

    public void setUrls(String urls) {
        this.urls = urls;
    }

    public Set<String> getSentinels() {
        return sentinels;
    }

    public void setSentinels(Set<String> sentinels) {
        this.sentinels = sentinels;
    }

    public RedisMode getRedisMode() {
        return redisMode;
    }

    public void setRedisMode(RedisMode redisMode) {
        this.redisMode = redisMode;
    }

    public int getRedisTimeout() {
        return redisTimeout;
    }

    public void setRedisTimeout(int redisTimeout) {
        this.redisTimeout = redisTimeout;
    }

    public HostAndPort getHap() {
        return hap;
    }

    public void setHap(HostAndPort hap) {
        this.hap = hap;
    }

    public boolean isRpsUseCache() {
        return rpsUseCache;
    }

    public void setRpsUseCache(boolean rpsUseCache) {
        this.rpsUseCache = rpsUseCache;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }
}
