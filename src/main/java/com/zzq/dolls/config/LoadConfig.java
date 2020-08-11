package com.zzq.dolls.config;

import java.nio.charset.*;
import org.apache.commons.io.*;
import java.io.*;
import java.util.stream.*;
import org.yaml.snakeyaml.*;
import java.util.*;
import java.util.Map.Entry;

import com.alibaba.fastjson.*;
import java.lang.reflect.*;

public class LoadConfig
{
    public static <T> void load(final Class<T> c) throws IOException {
        final From from = c.getAnnotation(From.class);
        if (from == null) {
            throw new IOException("adding annotations @From(value=\"config path\")");
        }
        final String name = from.name();
        final String[] names = from.alternateNames();
        if (name.equals("") && names.length == 0) {
            throw new IOException("config file path is null");
        }
        final FileType type = from.fileType();
        for (int i = names.length - 1; i >= 0; --i) {
            final File file = new File(names[i]);
            if (file.exists()) {
                load(new File(names[i]), c, type);
            }
        }
        if (!name.equals("")) {
            final File file2 = new File(name);
            if (file2.exists()) {
                load(new File(name), c, type);
            }
        }
    }
    
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
    
    public static <T> void load(final String json, final Class<T> c) {
        load((Map<String, Object>)JSONObject.parseObject(json), c);
    }
    
    public static <T> void load(final Map<String, Object> map, final Class<T> c) {
        final Field[] fields = c.getDeclaredFields();
        final Map<String, Object> lcMap = new HashMap<String, Object>();
        map.forEach((k, v) -> lcMap.put(k.toLowerCase(), v));
        for (final Field field : fields) {
            Object value = null;
            final From from = field.getAnnotation(From.class);
            Label_0666: {
                if (from != null) {
                    final String name = from.name().toLowerCase();
                    final String[] names = from.alternateNames();
                    boolean exist = false;
                    for (int i = names.length - 1; i >= 0; --i) {
                        if (lcMap.containsKey(names[i])) {
                            value = lcMap.get(names[i]);
                            exist = true;
                        }
                    }
                    if (lcMap.containsKey(name)) {
                        value = lcMap.get(name);
                        exist = true;
                    }
                    if (!exist) {
                        final boolean optional = from.must();
                        if (optional) {
                            throw new RuntimeException(c.getName() + " " + field.getName() + "(" + name + ") uninitialized");
                        }
                        break Label_0666;
                    }
                }
                else {
                    final String name = field.getName().toLowerCase();
                    value = lcMap.get(name);
                }
                field.setAccessible(true);
                try {
                    if (field.getType().isAssignableFrom(Integer.TYPE)) {
                        field.setInt(null, Integer.parseInt(value.toString()));
                    }
                    else if (field.getType().isAssignableFrom(Long.TYPE)) {
                        field.set(null, Long.parseLong(value.toString()));
                    }
                    else if (field.getType().isAssignableFrom(Double.TYPE)) {
                        field.set(null, Double.parseDouble(value.toString()));
                    }
                    else if (field.getType().isAssignableFrom(Float.TYPE)) {
                        field.set(null, Float.parseFloat(value.toString()));
                    }
                    else if (field.getType().isAssignableFrom(String.class)) {
                        if (value == null || value.toString().toLowerCase().equals("null") || value.toString().toLowerCase().equals("\"null\"")) {
                            field.set(null, null);
                        }
                        else {
                            final String tmp = value.toString();
                            if (tmp.startsWith("\"") && tmp.endsWith("\"")) {
                                value = tmp.substring(1, tmp.length() - 1);
                            }
                            field.set(null, value.toString());
                        }
                    }
                    else if (field.getType().isAssignableFrom(Boolean.TYPE)) {
                        field.set(null, Boolean.parseBoolean(value.toString()));
                    }
                    else if (field.getType().isAssignableFrom(List.class)) {
                        final Class<?> clazz = from.subClass();
                        field.set(null, JSON.parseArray(JSON.toJSONString(value), clazz));
                    }
                    else if (field.getType().isAssignableFrom(Map.class)) {
                        final Map<String, Object> tmp2 = (Map<String, Object>)JSONObject.parseObject(JSON.toJSONString(value));
                        field.set(null, tmp2);
                    }
                    else {
                        final Class<?> type = field.getType();
                        final Map<String, Object> tmp3 = (Map<String, Object>)JSONObject.parseObject(JSON.toJSONString(value));
                        load(tmp3, type);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    public static <T> String toString(final Class<T> c) {
        return toString(c, false);
    }
    
    public static <T> String toString(final Class<T> c, final boolean showFiledName) {
        final Field[] fields = c.getDeclaredFields();
        final StringBuilder sb = new StringBuilder("{\"" + c.getName() + "\":{");
        for (final Field field : fields) {
            final String name = field.getName();
            field.setAccessible(true);
            if (showFiledName) {
                sb.append("\"").append(name).append("\"").append(":");
            }
            else {
                final From from = field.getAnnotation(From.class);
                String fname;
                if (from != null) {
                    fname = from.name().toLowerCase();
                }
                else {
                    fname = field.getName().toLowerCase();
                }
                sb.append("\"").append(fname).append("\"").append(":");
            }
            try {
                final Object value = field.get(name);
                if (field.getType().isAssignableFrom(Integer.TYPE)) {
                    sb.append(value).append(",");
                }
                else if (field.getType().isAssignableFrom(Long.TYPE)) {
                    sb.append(value).append(",");
                }
                else if (field.getType().isAssignableFrom(Double.TYPE)) {
                    sb.append(value).append(",");
                }
                else if (field.getType().isAssignableFrom(Float.TYPE)) {
                    sb.append(value).append(",");
                }
                else if (field.getType().isAssignableFrom(String.class)) {
                    if (value == null) {
                        sb.append("null").append(",");
                    }
                    else {
                        sb.append("\"").append(value).append("\"").append(",");
                    }
                }
                else if (field.getType().isAssignableFrom(Boolean.TYPE)) {
                    sb.append(value).append(",");
                }
                else if (field.getType().isAssignableFrom(List.class)) {
                    sb.append(JSON.toJSONString(value)).append(",");
                }
                else if (field.getType().isAssignableFrom(Map.class)) {
                    sb.append(JSON.toJSONString(value)).append(",");
                }
                else {
                    final Class<?> type = field.getType();
                    sb.append(string(type, showFiledName)).append(",");
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.append("}}").toString();
    }
    
    private static <T> String string(final Class<T> c, final boolean showFiledName) {
        final Field[] fields = c.getDeclaredFields();
        final StringBuilder sb = new StringBuilder("{");
        for (final Field field : fields) {
            final String name = field.getName();
            field.setAccessible(true);
            if (showFiledName) {
                sb.append("\"").append(name).append("\"").append(":");
            }
            else {
                final From from = field.getAnnotation(From.class);
                String fname;
                if (from != null) {
                    fname = from.name().toLowerCase();
                }
                else {
                    fname = field.getName().toLowerCase();
                }
                sb.append("\"").append(fname).append("\"").append(":");
            }
            try {
                final Object value = field.get(name);
                if (field.getType().isAssignableFrom(Integer.TYPE)) {
                    sb.append(value).append(",");
                }
                else if (field.getType().isAssignableFrom(Long.TYPE)) {
                    sb.append(value).append(",");
                }
                else if (field.getType().isAssignableFrom(Double.TYPE)) {
                    sb.append(value).append(",");
                }
                else if (field.getType().isAssignableFrom(Float.TYPE)) {
                    sb.append(value).append(",");
                }
                else if (field.getType().isAssignableFrom(String.class)) {
                    if (value == null) {
                        sb.append("null").append(",");
                    }
                    else {
                        sb.append("\"").append(value).append("\"").append(",");
                    }
                }
                else if (field.getType().isAssignableFrom(Boolean.TYPE)) {
                    sb.append(value).append(",");
                }
                else if (field.getType().isAssignableFrom(List.class)) {
                    sb.append(JSON.toJSONString(value)).append(",");
                }
                else if (field.getType().isAssignableFrom(Map.class)) {
                    sb.append(JSON.toJSONString(value)).append(",");
                }
                else {
                    final Class<?> type = field.getType();
                    sb.append(string(type, showFiledName)).append(",");
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.append("}").toString();
    }
}
