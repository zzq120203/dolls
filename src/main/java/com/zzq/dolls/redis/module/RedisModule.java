package com.zzq.dolls.redis.module;

public enum RedisModule {
    Json(0), Graph(1), Search(2);

    private int id;

    private RedisModule(int id) {
        this.id = id;
    }

    public int getId() {
        return this.id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public static RedisModule create(int id) {
        switch (id) {
            case 0:
                return Json;
            case 1:
                return Graph;
            default:
                throw new RuntimeException("id is error");
        }
    }
}
