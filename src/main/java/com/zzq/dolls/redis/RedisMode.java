package com.zzq.dolls.redis;

/**
 * redis服务模式
 */
public enum RedisMode {
    STANDALONE(0), SENTINEL(1), CLUSTER(2);

    private int id;

    private RedisMode(int id) {
        this.id = id;
    }

    public int getId() {
        return this.id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public static RedisMode create(int id) {
        switch (id) {
            case 0:
                return STANDALONE;
            case 1:
                return SENTINEL;
            case 2:
                return CLUSTER;
            default:
                throw new RuntimeException("id is error");
        }
    }
}