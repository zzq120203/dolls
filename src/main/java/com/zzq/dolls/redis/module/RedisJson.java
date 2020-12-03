package com.zzq.dolls.redis.module;

import com.google.gson.Gson;
import com.redislabs.modules.rejson.JReJSON;
import com.redislabs.modules.rejson.Path;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

public class RedisJson extends JReJSON {

    private static final Gson gson = new Gson();

    private final Pool<Jedis> client;


    public RedisJson(Pool<Jedis> jedis) {
        super(jedis);
        this.client = jedis;
    }

    private enum Command implements ProtocolCommand {
        ARRAPPEND("JSON.ARRAPPEND");
        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    public void arrAppend(String key, Object object, Path path) {
        arrAppend(key, object, ExistenceModifier.DEFAULT, path);
    }

    public void arrAppend(String key, Object object, ExistenceModifier flag, Path path) {

        List<byte[]> args = new ArrayList<>(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        if (ExistenceModifier.DEFAULT != flag) {
            args.add(flag.getRaw());
        }

        String status;
        try (Jedis conn = getConnection()) {
            conn.getClient()
                    .sendCommand(Command.ARRAPPEND, args.toArray(new byte[args.size()][]));
            status = conn.getClient().getStatusCodeReply();
        }
        assertReplyOK(status);
    }

    private Jedis getConnection() {
        return this.client.getResource();
    }

    /**
     * Helper to check for an OK reply
     * @param str the reply string to "scrutinize"
     */
    private static void assertReplyOK(final String str) {
        if (!str.equals("OK"))
            throw new RuntimeException(str);
    }

}
