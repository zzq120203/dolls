package zzq.dolls.config;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface From {
    /**
     * @return the desired name of the field when it is serialized or deserialized
     */
    String value();
}