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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 配置类静态属性加载
 * @author zhangzhanqi
 */
public class LoadConfig {

    private static Gson gson = new GsonBuilder().registerTypeAdapter(new TypeToken<Map<String, Object>>() {
    }.getType(), (JsonDeserializer<Map<String, Object>>) (json, typeOfT, context) -> {
        Map<String, Object> map = json.getAsJsonObject().entrySet().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        return map;
    }).create();


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
        Map<String, Object> lcMap = map.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toLowerCase(), Entry::getValue));
        Arrays.stream(fields)
                .collect(Collectors.toMap(LoadConfig::getFieldName, field -> field))
                .forEach((name, field) -> {
                    if (!lcMap.containsKey(name)) {
                        Optional optional = field.getAnnotation(Optional.class);
                        if (optional == null)
                            throw new RuntimeException(field.getName().toLowerCase() + " uninitialized");
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
                            } else if (field.getType().isAssignableFrom(String.class)) {
                                field.set(null, value.toString());
                            } else if (field.getType().isAssignableFrom(boolean.class)) {
                                field.set(null, Boolean.parseBoolean(value.toString()));
                            } else if (field.getType().isAssignableFrom(List.class)) {
                                field.set(null, gson.fromJson(gson.toJson(value), new TypeToken<List<String>>() {
                                }.getType()));
                            } else if (field.getType().isAssignableFrom(Map.class)) {
                                field.set(null, gson.fromJson(gson.toJson(value), new TypeToken<Map<String, String>>() {
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

    private static String getFieldName(Field field) {
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
        Field[] fields = c.getDeclaredFields();
        StringBuilder sb = new StringBuilder("{\"" + c.getName() + "\":{");
        Arrays.stream(fields)
                .collect(Collectors.toMap(Field::getName, field -> field))
                .forEach((name, field) -> {
                    field.setAccessible(true);
                    sb.append("\"").append(name).append("\"").append(":");
                    try {
                        if (field.getType().isAssignableFrom(int.class)) {
                            sb.append(field.get(name)).append(",");
                        } else if (field.getType().isAssignableFrom(long.class)) {
                            sb.append(field.get(name)).append(",");
                        } else if (field.getType().isAssignableFrom(double.class)) {
                            sb.append(field.get(name)).append(",");
                        } else if (field.getType().isAssignableFrom(String.class)) {
                            sb.append("\"").append(field.get(name)).append("\"").append(",");
                        } else if (field.getType().isAssignableFrom(boolean.class)) {
                            sb.append(field.get(name)).append(",");
                        } else if (field.getType().isAssignableFrom(List.class)) {
                            sb.append(gson.toJson(field.get(name))).append(",");
                        } else if (field.getType().isAssignableFrom(Map.class)) {
                            sb.append(gson.toJson(field.get(name))).append(",");
                        } else {
                            sb.append("\"").append("unknown").append("\"").append(",");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
        sb.deleteCharAt(sb.length() -1);
        return sb.append("}}").toString();
    }
}
