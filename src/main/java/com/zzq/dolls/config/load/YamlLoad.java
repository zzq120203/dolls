package com.zzq.dolls.config.load;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class YamlLoad {

    public Map<String, Object> LoadFile(File file) throws IOException {
        try (FileInputStream stream = new FileInputStream(file)) {
            Yaml yaml = new Yaml();
            return yaml.load(stream);
        }
    }
    
}
