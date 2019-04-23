package zzq.dolls.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * 配置类静态属性加载
 * @author zhangzhanqi
 */
public class LoadConfig {

    static Gson gson = new GsonBuilder().registerTypeAdapter(new TypeToken<Map<String, Object>>() {
    }.getType(), (JsonDeserializer<Map<String, Object>>) (json, typeOfT, context) -> {
        Map<String, Object> map = json.getAsJsonObject().entrySet().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        return map;
    }).serializeNulls().create();


    public static <T> void load(File file, Class<T> c, FileType type) throws IOException {
        switch (type) {
            case JSON: {
                String json = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
                load(json, c);
                break;
            }
            case PROPERTIES: {
                Properties props = new Properties();
                try (FileInputStream stream = new FileInputStream(file)) {
                    props.load(stream);
                    Map<String, Object> map = props.entrySet().stream()
                            .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
                    load(map, c);
                }
                break;
            }
            case XML: {
                Properties props = new Properties();
                try (FileInputStream stream = new FileInputStream(file)) {
                    props.loadFromXML(stream);
                    Map<String, Object> map = props.entrySet().stream()
                            .collect(Collectors.toMap(e -> e.getKey().toString(), Entry::getValue));
                    load(map, c);
                }
                break;
            }
            case YAML: {
                try (FileInputStream stream = new FileInputStream(file)) {
                    Yaml yaml = new Yaml();
                    Map<String, Object> map = yaml.load(stream);
                    load(map, c);
                }
                break;
            }
            case HOCON: {
                throw new IOException("undefinition");
            }
            case INI: {
                throw new IOException("undefinition");
            }
        }
    }

    public static <T> void load(String json, Class<T> c) {
        Map<String, Object> map = gson.fromJson(json, new TypeToken<Map<String, Object>>() {
        }.getType());
        load(map, c);
    }

    public static <T> void load(Map<String, Object> map, Class<T> c) {
        Field[] fields = c.getDeclaredFields();
        Map<String, Object> lcMap = new HashMap<>();
        map.forEach((k, v) -> lcMap.put(k.toLowerCase(), v));
        for (Field field : fields) {
            String name = getFieldName(field);
            if (!lcMap.containsKey(name)) {
                Optional optional = field.getAnnotation(Optional.class);
                if (optional == null)
                    throw new RuntimeException(c.getName() + " " + field.getName() + "(" + name + ") uninitialized");
            } else {
                Object value = lcMap.get(name);
                field.setAccessible(true);
                try {
                    if (field.getType().isAssignableFrom(int.class)) {
                        field.setInt(null, Integer.parseInt(value.toString()));
                    } else if (field.getType().isAssignableFrom(long.class)) {
                        field.set(null, Long.parseLong(value.toString()));
                    } else if (field.getType().isAssignableFrom(double.class)) {
                        field.set(null, Double.parseDouble(value.toString()));
                    } else if (field.getType().isAssignableFrom(float.class)) {
                        field.set(null, Float.parseFloat(value.toString()));
                    } else if (field.getType().isAssignableFrom(String.class)) {
                        if (value == null || value.toString().toLowerCase().equals("null") || value.toString().toLowerCase().equals("\"null\"")) {
                            field.set(null, null);
                        } else {
                            String tmp = value.toString();
                            if (tmp.startsWith("\"") && tmp.endsWith("\"")) {
                                value = tmp.substring(1, tmp.length() - 1);
                            }
                            field.set(null, value);
                        }
                    } else if (field.getType().isAssignableFrom(boolean.class)) {
                        field.set(null, Boolean.parseBoolean(value.toString()));
                    } else if (field.getType().isAssignableFrom(List.class)) {
                        field.set(null, gson.fromJson(gson.toJson(value), new TypeToken<List<String>>() {
                        }.getType()));
                    } else if (field.getType().isAssignableFrom(Map.class)) {
                        field.set(null, gson.fromJson(gson.toJson(value), new TypeToken<Map<String, String>>() {
                        }.getType()));
                    } else {
                        Class<?> type = field.getType();
                        Map<String, Object> tmp = gson.fromJson(gson.toJson(value), new TypeToken<Map<String, Object>>() {}.getType());
                        load(tmp, type);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static <T> void loadStringMap(Map<String, String> map, Class<T> c) {
        Field[] fields = c.getDeclaredFields();
        Map<String, String> lcMap = new HashMap<>();
        map.forEach((k, v) -> lcMap.put(k.toLowerCase(), v));
        for (Field field : fields) {
            String name = getFieldName(field);
            if (!lcMap.containsKey(name)) {
                Optional optional = field.getAnnotation(Optional.class);
                if (optional == null)
                    throw new RuntimeException(c.getName() + " " + field.getName() + "(" + name + ") uninitialized");
            } else {
                String value = lcMap.get(name);
                field.setAccessible(true);
                try {
                    if (field.getType().isAssignableFrom(int.class)) {
                        field.setInt(null, Integer.parseInt(value));
                    } else if (field.getType().isAssignableFrom(long.class)) {
                        field.set(null, Long.parseLong(value));
                    } else if (field.getType().isAssignableFrom(double.class)) {
                        field.set(null, Double.parseDouble(value));
                    } else if (field.getType().isAssignableFrom(float.class)) {
                        field.set(null, Float.parseFloat(value));
                    } else if (field.getType().isAssignableFrom(String.class)) {
                        if (value == null || value.toLowerCase().equals("null") || value.toLowerCase().equals("\"null\"")) {
                            field.set(null, null);
                        }
                        else {
                            String tmp = value;
                            if (tmp.startsWith("\"") && tmp.endsWith("\"")) {
                                value = tmp.substring(1, tmp.length() - 1);
                            }
                            field.set(null, value);
                        }
                    } else if (field.getType().isAssignableFrom(boolean.class)) {
                        field.set(null, Boolean.parseBoolean(value));
                    } else if (field.getType().isAssignableFrom(List.class)) {
                        field.set(null, gson.fromJson(value, new TypeToken<List<String>>() {
                        }.getType()));
                    } else if (field.getType().isAssignableFrom(Map.class)) {
                        field.set(null, gson.fromJson(value, new TypeToken<Map<String, String>>() {
                        }.getType()));
                    } else {
                        Class<?> type = field.getType();
                        Map<String, String> tmp = gson.fromJson(gson.toJson(value), new TypeToken<Map<String, String>>() {}.getType());
                        loadStringMap(tmp, type);
                    }
                } catch (Exception e) {
                    System.out.println(field.getType());
                    e.printStackTrace();
                }
            }
        }
    }

    static String getFieldName(Field field) {
        From from = field.getAnnotation(From.class);
        if (from != null) {
            return from.value().toLowerCase();
        }
        SerializedName serializedName = field.getAnnotation(SerializedName.class);
        if (serializedName != null) {
            return serializedName.value().toLowerCase();
        }
        return field.getName().toLowerCase();
    }

    public static <T> String toString(Class<T> c) {
        return toString(c, false);
    }

    public static <T> String toString(Class<T> c, boolean showFiledName) {
        Field[] fields = c.getDeclaredFields();
        StringBuilder sb = new StringBuilder("{\"" + c.getName() + "\":{");
        for (Field field : fields) {
            String name = field.getName();
            field.setAccessible(true);
            if (showFiledName) sb.append("\"").append(name).append("\"").append(":");
            else sb.append("\"").append(getFieldName(field)).append("\"").append(":");
            try {
                Object value = field.get(name);
                if (field.getType().isAssignableFrom(int.class)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(long.class)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(double.class)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(float.class)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(String.class)) {
                    if (value == null) sb.append("null").append(",");
                    else sb.append("\"").append(value).append("\"").append(",");
                } else if (field.getType().isAssignableFrom(boolean.class)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(List.class)) {
                    sb.append(gson.toJson(value)).append(",");
                } else if (field.getType().isAssignableFrom(Map.class)) {
                    sb.append(gson.toJson(value)).append(",");
                } else {
                    Class<?> type = field.getType();
                    sb.append(string(type, showFiledName)).append(",");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        sb.deleteCharAt(sb.length() -1);
        return sb.append("}}").toString();
    }

    private static <T> String string(Class<T> c, boolean showFiledName) {
        Field[] fields = c.getDeclaredFields();
        StringBuilder sb = new StringBuilder("{");
        for (Field field : fields) {
            String name = field.getName();
            field.setAccessible(true);
            if (showFiledName) sb.append("\"").append(name).append("\"").append(":");
            else sb.append("\"").append(getFieldName(field)).append("\"").append(":");
            try {
                Object value = field.get(name);
                if (field.getType().isAssignableFrom(int.class)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(long.class)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(double.class)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(float.class)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(String.class)) {
                    if (value == null) sb.append("null").append(",");
                    else sb.append("\"").append(value).append("\"").append(",");
                } else if (field.getType().isAssignableFrom(boolean.class)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(List.class)) {
                    sb.append(gson.toJson(value)).append(",");
                } else if (field.getType().isAssignableFrom(Map.class)) {
                    sb.append(gson.toJson(value)).append(",");
                } else {
                    Class<?> type = field.getType();
                    sb.append(string(type, showFiledName)).append(",");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        sb.deleteCharAt(sb.length() -1);
        return sb.append("}").toString();
    }
}
