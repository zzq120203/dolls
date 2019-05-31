package zzq.dolls.config;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface From {
    /**
     * @return the desired name of the field when it is serialized or deserialized
     */
    String value();

    FileType fileType() default FileType.YAML;
}