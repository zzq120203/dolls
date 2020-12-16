package com.zzq.dolls.redis.module;

import redis.clients.jedis.Client;
import redis.clients.jedis.PipelineBase;

import java.io.Closeable;

public class ModulePipeline extends PipelineBase implements Closeable {

    protected Client client = null;

    public void setClient(Client client) {
        this.client = client;
    }

    @Override
    protected Client getClient(String key) {
        return client;
    }

    @Override
    protected Client getClient(byte[] key) {
        return client;
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
