package com.zzq.dolls.redis.exception;

import redis.clients.jedis.exceptions.JedisDataException;

public class KeyNotFoundException extends JedisDataException {
    public KeyNotFoundException(String message) {
        super(message);
    }

    public KeyNotFoundException(Throwable cause) {
        super(cause);
    }

    public KeyNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
