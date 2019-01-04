package zzq.dolls.config;

import com.google.gson.reflect.TypeToken;
import io.javalin.Context;
import io.javalin.Javalin;
import zzq.dolls.redis.RedisPool;

import java.lang.reflect.Field;
import java.util.*;

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
        Field[] fields = c.getDeclaredFields();
        Map<String, Field> map = new HashMap<>();
        for (Field field : fields) {
            map.put(LoadConfig.getFieldName(field), field);
        }
        app.error(404, ctx -> ctx.result(LoadConfig.gson.toJson(map.keySet())));
        app.routes(() -> {
            path("update", () -> path(c.getName().toLowerCase(), () -> {
                map.forEach((name, field) -> {
                    get(name +"/:" + name, ctx -> update(ctx, field, name, c.getName()));
                });
            }));
            get("config", (context) -> context.result(LoadConfig.toString(c)));
        });
    }

    private void update(Context context, Field field, String path, String name) throws Exception {
        String value = context.pathParam(path);
        if (rpool != null) {
            rpool.jedis(jedis -> jedis.hset("config." + name, path, value));
        }
        field.setAccessible(true);
        if (field.getType().isAssignableFrom(int.class)) {
            field.setInt(null, Integer.parseInt(value));
            context.result("{\"" + path + "\" : " + value + "}");
        } else if (field.getType().isAssignableFrom(long.class)) {
            field.set(null, Long.parseLong(value));
            context.result("{\"" + path + "\" : " + value + "}");
        } else if (field.getType().isAssignableFrom(double.class)) {
            field.set(null, Double.parseDouble(value));
            context.result("{\"" + path + "\" : " + value + "}");
        } else if (field.getType().isAssignableFrom(String.class)) {
            if (value.toLowerCase().equals("null")) {
                field.set(null, null);
                context.result("{\"" + path + "\" : null}");
            } else {
                field.set(null, value);
                context.result("{\"" + path + "\" : \"" + value + "\"}");
            }
        } else if (field.getType().isAssignableFrom(boolean.class)) {
            field.set(null, Boolean.parseBoolean(value));
            context.result("{\"" + path + "\" : " + value + "}");
        } else if (field.getType().isAssignableFrom(List.class)) {
            field.set(null, LoadConfig.gson.fromJson(value, new TypeToken<List<String>>() {
            }.getType()));
            context.result("{\"" + path + "\" : " + value + "}");
        } else if (field.getType().isAssignableFrom(Map.class)) {
            field.set(null, LoadConfig.gson.fromJson(value, new TypeToken<Map<String, String>>() {
            }.getType()));
            context.result("{\"" + path + "\" : " + value + "}");
        } else {
            throw new RuntimeException("unknown data type exception -> " + field.getType());
        }
    }

    public <T> void save(Class<T> c) {
        Field[] fields = c.getDeclaredFields();
        rpool.jedis(jedis -> {
            for (Field field : fields) {
                String name = LoadConfig.getFieldName(field);
                field.setAccessible(true);
                try {
                    Object value = field.get(field.getName());
                    if (field.getType().isAssignableFrom(int.class)) {
                        jedis.hset("config." + c.getName(), name, value.toString());
                    } else if (field.getType().isAssignableFrom(long.class)) {
                        jedis.hset("config." + c.getName(), name, value.toString());
                    } else if (field.getType().isAssignableFrom(double.class)) {
                        jedis.hset("config." + c.getName(), name, value.toString());
                    } else if (field.getType().isAssignableFrom(String.class)) {
                        if (value == null) jedis.hset("config." + c.getName(), name, "null");
                        else jedis.hset("config." + c.getName(), name, value.toString());
                    } else if (field.getType().isAssignableFrom(boolean.class)) {
                        jedis.hset("config." + c.getName(), name, value.toString());
                    } else if (field.getType().isAssignableFrom(List.class)) {
                        jedis.hset("config." + c.getName(), name, LoadConfig.gson.toJson(value));
                    } else if (field.getType().isAssignableFrom(Map.class)) {
                        jedis.hset("config." + c.getName(), name, LoadConfig.gson.toJson(value));
                    } else {
                        throw new RuntimeException("unknown data type exception -> " + field.getType());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return null;
        });
    }

    public <T> void update(Class<T> c) throws InterruptedException {
        update(c, 1000 * 10);
    }

    public <T> void update(Class<T> c, long millis) throws InterruptedException {
        do {
            Map<String, String> map = rpool.jedis(jedis -> jedis.hgetAll("config." + c.getName()));

            LoadConfig.loadStringMap(map, c);
            Thread.sleep(millis);
        } while (true);
    }
}
