package com.zzq.dolls.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zzq.dolls.config.load.YamlLoad;

import org.apache.commons.io.FileUtils;

public class LoadConfig {
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
                YamlLoad yaml = new YamlLoad();
                Map<String, Object> map = yaml.LoadFile(file);
                load(map, c);
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
        load((Map<String, Object>) JSONObject.parseObject(json), c);
    }

    public static <T> void load(Map<String, Object> map, Class<T> c) {
        Field[] fields = c.getDeclaredFields();
        Map<String, Object> lcMap = new HashMap<String, Object>();
        map.forEach((k, v) -> lcMap.put(k.toLowerCase(), v));
        for (Field field : fields) {
            Object value = null;
            From from = field.getAnnotation(From.class);
            if (from != null) {
                String[] names = from.alternateNames();
                boolean exist = false;
                for (int i = names.length - 1; i >= 0; --i) {
                    String name = names[i].toLowerCase();
                    if (lcMap.containsKey(name)) {
                        value = lcMap.get(name);
                        exist = true;
                    }
                }
                String name = from.name().toLowerCase();
                if (name.isEmpty()) {
                    name = field.getName().toLowerCase();
                }
                if (lcMap.containsKey(name)) {
                    value = lcMap.get(name);
                    exist = true;
                }
                if (!exist) {
                    boolean must = from.must();
                    if (must) {
                        throw new RuntimeException(
                                c.getName() + " " + field.getName() + "(" + name + ") uninitialized");
                    }
                    continue;
                }
            } else {
                String name = field.getName().toLowerCase();
                value = lcMap.get(name);
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
                } else if (field.getType().isAssignableFrom(List.class)) {
                    Class<?> clazz = from.subClass();
                    field.set(null, JSON.parseArray(JSON.toJSONString(value), clazz));
                } else if (field.getType().isAssignableFrom(Map.class)) {
                    Map<String, Object> tmp2 = (Map<String, Object>) JSONObject.parseObject(JSON.toJSONString(value));
                    field.set(null, tmp2);
                } else {
                    Class<?> type = field.getType();
                    Map<String, Object> tmp3 = (Map<String, Object>) JSONObject.parseObject(JSON.toJSONString(value));
                    load(tmp3, type);
                }
            } catch (Exception e) {
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
                } else if (field.getType().isAssignableFrom(List.class)) {
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
                } else if (field.getType().isAssignableFrom(List.class)) {
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
