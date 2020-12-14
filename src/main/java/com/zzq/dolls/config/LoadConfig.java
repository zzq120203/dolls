package com.zzq.dolls.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.commons.io.FileUtils;
import org.yaml.snakeyaml.Yaml;

public class LoadConfig {

    @SafeVarargs
    public static <T> void load(Class<T>... cs) throws IOException {
        for (Class<T> c : cs) {
            load(c);
        }
    }

    public static <T> void load(Class<T> c) throws IOException {
        From from = c.getAnnotation(From.class);
        if (from == null) {
            throw new IOException("adding annotations @From(value=\"config path\")");
        }
        String name = from.name();
        String[] names = from.alternateNames();
        if (name.equals("") && names.length == 0) {
            throw new IOException("config file path is null");
        }
        FileType type = from.fileType();
        for (int i = names.length - 1; i >= 0; --i) {
            File file = new File(names[i]);
            if (file.exists()) {
                load(new File(names[i]), c, type);
            }
        }
        if (!name.equals("")) {
            File file2 = new File(name);
            if (file2.exists()) {
                load(new File(name), c, type);
            }
        }
    }

    public static <T> void load(File file, Class<T> c, FileType type) throws IOException {
        if (!file.exists()) {
            throw new IOException("config file(" + file.getPath() + ") is not exist");
        }
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
            case HOCON:
            case INI: {
                throw new IOException("undefinition");
            }
        }
    }

    public static <T> void load(String json, Class<T> c) {
        load(JSONObject.parseObject(json), c);
    }

    public static <T> void load(Map<String, Object> map, Class<T> c) {
        Field[] fields = c.getDeclaredFields();
        Map<String, Object> lcMap = new HashMap<>();
        map.forEach((k, v) -> lcMap.put(k.toLowerCase(), v));
        for (Field field : fields) {
            if (field.getType().isAssignableFrom(c)) {
                continue;
            }
            Object value = null;
            From from = field.getAnnotation(From.class);
            boolean exist = false;
            String name = field.getName().toLowerCase();
            if (lcMap.containsKey(name)) {
                value = lcMap.get(name);
                exist = true;
            } else if (from != null) {
                name = from.name().toLowerCase();
                if (!name.isEmpty() && lcMap.containsKey(name)) {
                    value = lcMap.get(name);
                    exist = true;
                } else {
                    String[] names = from.alternateNames();
                    for (String a_name: names) {
                        if (lcMap.containsKey(a_name)) {
                            value = lcMap.get(a_name);
                            exist = true;
                        }
                    }
                }
            }
            if (!exist) {
                boolean must = from == null || from.must();
                if (must) {
                    throw new RuntimeException(
                            c.getName() + " " + field.getName() + "(" + name + ") uninitialized");
                }
                continue;
            }
            field.setAccessible(true);
            try {
                if (field.getType().isAssignableFrom(Integer.TYPE)) {
                    field.setInt(null, Integer.parseInt(value.toString()));
                } else if (field.getType().isAssignableFrom(Long.TYPE)) {
                    field.set(null, Long.parseLong(value.toString()));
                } else if (field.getType().isAssignableFrom(Double.TYPE)) {
                    field.set(null, Double.parseDouble(value.toString()));
                } else if (field.getType().isAssignableFrom(Float.TYPE)) {
                    field.set(null, Float.parseFloat(value.toString()));
                } else if (field.getType().isAssignableFrom(String.class)) {
                    if (value == null || value.toString().toLowerCase().equals("null")
                            || value.toString().toLowerCase().equals("\"null\"")) {
                        field.set(null, null);
                    } else {
                        String tmp = value.toString();
                        if (tmp.startsWith("\"") && tmp.endsWith("\"")) {
                            value = tmp.substring(1, tmp.length() - 1);
                        }
                        field.set(null, value.toString());
                    }
                } else if (field.getType().isAssignableFrom(Boolean.TYPE)) {
                    field.set(null, Boolean.parseBoolean(value.toString()));
                } else if (Collection.class.isAssignableFrom(field.getType())) {
                    Class<?> clazz = from == null ? Object.class : from.subClass();
                    field.set(null, JSON.parseArray(JSON.toJSONString(value), clazz));
                } else if (field.getType().isAssignableFrom(Map.class)) {
                    Map<String, Object> tmp2 = JSONObject.parseObject(JSON.toJSONString(value));
                    field.set(null, tmp2);
                } else {
                    Class<?> type = field.getType();
                    Map<String, Object> tmp3 = JSONObject.parseObject(JSON.toJSONString(value));
                    load(tmp3, type);
                }
            } catch (Exception e) {
                System.out.println(field.getName() + " is error:");
                e.printStackTrace();
            }
        }
    }

    public static <T> String toString(Class<T> c) {
        return toString(c, true);
    }

    public static <T> String toString(Class<T> c, boolean showFiledName) {
        Field[] fields = c.getDeclaredFields();
        StringBuilder sb = new StringBuilder("{\"" + c.getName() + "\":{");
        for (Field field : fields) {
            if (field.getType().isAssignableFrom(c)) {
                continue;
            }
            String name = field.getName();
            field.setAccessible(true);
            if (showFiledName) {
                sb.append("\"").append(name).append("\"").append(":");
            } else {
                From from = field.getAnnotation(From.class);
                String fname = null;
                if (from != null) {
                    fname = from.name().toLowerCase();
                    if (fname.isEmpty()) {
                        String[] names = from.alternateNames();
                        if (names.length > 0) {
                            fname = names[1];
                        }
                    }
                } 
                if (fname == null) {
                    fname = field.getName().toLowerCase();
                }
                sb.append("\"").append(fname).append("\"").append(":");
            }
            try {
                Object value = field.get(name);
                if (field.getType().isAssignableFrom(Integer.TYPE)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(Long.TYPE)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(Double.TYPE)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(Float.TYPE)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(String.class)) {
                    if (value == null) {
                        sb.append("null").append(",");
                    } else {
                        sb.append("\"").append(value).append("\"").append(",");
                    }
                } else if (field.getType().isAssignableFrom(Boolean.TYPE)) {
                    sb.append(value).append(",");
                } else if (Collection.class.isAssignableFrom(field.getType())) {
                    sb.append(JSON.toJSONString(value)).append(",");
                } else if (field.getType().isAssignableFrom(Map.class)) {
                    sb.append(JSON.toJSONString(value)).append(",");
                } else {
                    final Class<?> type = field.getType();
                    sb.append(string(type, showFiledName)).append(",");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.append("}}").toString();
    }

    private static <T> String string(Class<T> c, boolean showFiledName) {
        Field[] fields = c.getDeclaredFields();
        StringBuilder sb = new StringBuilder("{");
        for (Field field : fields) {
            if (field.getType().isAssignableFrom(c)) {
                continue;
            }
            String name = field.getName();
            field.setAccessible(true);
            if (showFiledName) {
                sb.append("\"").append(name).append("\"").append(":");
            } else {
                From from = field.getAnnotation(From.class);
                String fname = null;
                if (from != null) {
                    fname = from.name().toLowerCase();
                    if (fname.isEmpty()) {
                        String[] names = from.alternateNames();
                        if (names.length > 0) {
                            fname = names[1];
                        }
                    }
                } 
                if (fname == null) {
                    fname = field.getName().toLowerCase();
                }
                sb.append("\"").append(fname).append("\"").append(":");
            }
            try {
                Object value = field.get(name);
                if (field.getType().isAssignableFrom(Integer.TYPE)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(Long.TYPE)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(Double.TYPE)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(Float.TYPE)) {
                    sb.append(value).append(",");
                } else if (field.getType().isAssignableFrom(String.class)) {
                    if (value == null) {
                        sb.append("null").append(",");
                    } else {
                        sb.append("\"").append(value).append("\"").append(",");
                    }
                } else if (field.getType().isAssignableFrom(Boolean.TYPE)) {
                    sb.append(value).append(",");
                } else if (Collection.class.isAssignableFrom(field.getType())) {
                    sb.append(JSON.toJSONString(value)).append(",");
                } else if (field.getType().isAssignableFrom(Map.class)) {
                    sb.append(JSON.toJSONString(value)).append(",");
                } else {
                    Class<?> type = field.getType();
                    sb.append(string(type, showFiledName)).append(",");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.append("}").toString();
    }
}
