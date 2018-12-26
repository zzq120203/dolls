package zzq.dolls.config;

import com.google.gson.reflect.TypeToken;
import io.javalin.Context;
import io.javalin.Javalin;
import zzq.dolls.redis.RedisPool;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.path;

public class UpdateConfig {

    private RedisPool rpool;

    private Javalin app;

    public UpdateConfig(RedisPool pool, Javalin app) {
        this.rpool = pool;
        this.app = app;
    }

    public <T> void http(Class<T> c) {
        app.routes(() -> {
            path("update", () -> path(c.getName(), () -> Arrays.stream(c.getDeclaredFields()).forEach(field -> get(":" + field.getName(), ctx -> update(ctx, field, c.getName())))));
            get("config", (context) -> context.result(LoadConfig.toString(c)));
        });
    }

    private void update(Context context, Field field, String name) throws Exception {
        String value = context.pathParam(field.getName());
        if (rpool != null) {
            rpool.jedis(jedis -> jedis.hset("config." + name, field.getName(), value));
        }
        field.setAccessible(true);
        if (field.getType().isAssignableFrom(int.class)) {
            field.setInt(null, Integer.parseInt(value));
        } else if (field.getType().isAssignableFrom(long.class)) {
            field.set(null, Long.parseLong(value));
        } else if (field.getType().isAssignableFrom(double.class)) {
            field.set(null, Double.parseDouble(value));
        } else if (field.getType().isAssignableFrom(String.class)) {
            field.set(null, value);
        } else if (field.getType().isAssignableFrom(boolean.class)) {
            field.set(null, Boolean.parseBoolean(value));
        } else if (field.getType().isAssignableFrom(List.class)) {
            field.set(null, LoadConfig.gson.fromJson(value, new TypeToken<List<String>>() {
            }.getType()));
        } else if (field.getType().isAssignableFrom(Map.class)) {
            field.set(null, LoadConfig.gson.fromJson(value, new TypeToken<Map<String, String>>() {
            }.getType()));
        } else {
            throw new RuntimeException("unknown data type exception -> " + field.getType());
        }
    }

    public <T> void update(Class<T> c) {
        Map<String, String> map = rpool.jedis(jedis -> jedis.hgetAll("config." + c.getName()));
        LoadConfig.load(LoadConfig.gson.toJson(map), c);
    }

}
