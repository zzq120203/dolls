package com.zzq.dolls.config;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.TYPE })
public @interface From {
    String name();
    
    boolean must() default true;
    
    String[] alternateNames() default {};
    
    FileType fileType() default FileType.YAML;
    
    Class<?> subClass() default Object.class;
}
