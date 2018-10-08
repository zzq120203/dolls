package cn.ac.iie.dolls.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LoadConfig {

    private static Gson gson = new GsonBuilder().registerTypeAdapter(new TypeToken<Map<String, Object>>() {
    }.getType(), (JsonDeserializer<Map<String, Object>>) (json, typeOfT, context) -> {
        Map<String, Object> map = json.getAsJsonObject().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return map;
    }).create();


    public static <T> void load(File file, Class<T> c) throws IOException {
        String json = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        load(json, c);
    }

    public static <T> void load(String json, Class<T> c) {
        Map<String, String> map = gson.fromJson(json, new TypeToken<Map<String, String>>() {
        }.getType());
        load(map, c);
    }

    public static <T> void load(Map<String, String> map, Class<T> c) {
        Field[] fields = c.getDeclaredFields();
        Map<String, String> lcMap = map.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toLowerCase(), Map.Entry::getValue));
        Arrays.stream(fields)
                .collect(Collectors.toMap(field -> field.getName().toLowerCase(), field -> field))
                .forEach((name, field) -> {
                    if (!lcMap.containsKey(name)) {
                        Optional optional = field.getAnnotation(Optional.class);
                        if (optional == null)
                            throw new RuntimeException(field.getName().toLowerCase() + " uninitialized");
                    } else {
                        Object value = lcMap.get(name);
                        field.setAccessible(true);
                        try {
                            if (field.getType() == int.class) {
                                field.setInt(null, Integer.parseInt(value.toString()));
                            } else if (field.getType() == long.class) {
                                field.set(null, Long.parseLong(value.toString()));
                            } else if (field.getType() == double.class) {
                                field.set(null, Double.parseDouble(value.toString()));
                            } else if (field.getType() == String.class) {
                                field.set(null, value.toString());
                            } else if (field.getType() == List.class) {
                                field.set(null, gson.fromJson(value.toString(), new TypeToken<List<String>>() {
                                }.getType()));
                            } else if (field.getType() == Map.class) {
                                field.set(null, gson.fromJson(value.toString(), new TypeToken<Map<String, String>>() {
                                }.getType()));
                            } else {
                                throw new RuntimeException("unknown data type exception -> " + field.getType());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
    }

}
