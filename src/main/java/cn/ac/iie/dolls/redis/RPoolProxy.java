package cn.ac.iie.dolls.redis;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.HashSet;

public class RPoolProxy {
    public MMConf conf;
    public RedisPool rpL1 = null;
    public RedisPoolSelector rps = null;

    private Boolean isInit = false;

    public RPoolProxy(MMConf conf) {
        if (conf != null)
            this.conf = conf;
        else {
            this.conf = new MMConf();
        }
    }

    public RPoolProxy init() throws Exception {
        return init(conf.getUrls(), conf.getAuthToken());
    }

    public RPoolProxy init(String urls, String authToken) throws Exception {
        if (urls == null) {
            throw new Exception("The RPool url can not be null.");
        }
        if (isInit) {
            return this;
        }
        isInit = true;

        conf.setAuthToken(authToken);

        if (urls.startsWith("STL://")) {
            urls = urls.substring(6);
            conf.setRedisMode(MMConf.RedisMode.SENTINEL);
        } else if (urls.startsWith("STA://")) {
            urls = urls.substring(6);
            conf.setRedisMode(MMConf.RedisMode.STANDALONE);
        }
        switch (conf.getRedisMode()) {
            case SENTINEL:
                init_by_sentinel(conf, urls);
                break;
            case STANDALONE:
                init_by_standalone(conf, urls);
                break;
            case CLUSTER:
            default:
                System.out.println("RPoolProxy do NOT support CLUSTER or other mode now, " +
                        "use STL/STA instead.");
                break;
        }

        return this;
    }

    private int init_by_sentinel(MMConf conf, String urls) throws Exception {
        if (conf.getRedisMode() != MMConf.RedisMode.SENTINEL)
            return -1;

        // iterate the sentinel set, get master IP:port, save to sentinel set
        if (conf.getSentinels() == null) {
            if (urls == null) {
                throw new Exception("Invalid URL(null) or sentinels.");
            }
            HashSet<String> sens = new HashSet<String>();
            String[] s = urls.split(";");

            for (int i = 0; i < s.length; i++) {
                sens.add(s[i]);
            }
            conf.setSentinels(sens);
        }
        rpL1 = new RedisPool(conf, "l1.master");
        rps = new RedisPoolSelector(conf, rpL1);

        return 0;
    }

    private int init_by_standalone(MMConf conf2, String urls) throws Exception {
        if (conf.getRedisMode() != MMConf.RedisMode.STANDALONE) {
            return -1;
        }
        // get IP:port, save it to HaP
        if (urls == null) {
            throw new Exception("Invalid URL: null");
        }
        String[] s = urls.split(":");
        if (s != null && s.length == 2) {
            try {
                HostAndPort hap = new HostAndPort(s[0],
                        Integer.parseInt(s[1]));
                conf.setHap(hap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        rpL1 = new RedisPool(conf, "nomaster");
        rps = new RedisPoolSelector(conf, rpL1);

        return 0;
    }

    public <T> T jedis(IRedis<T> r) {
        Jedis jedis = rpL1.getResource();
        try {
            if (jedis != null) return r.a(jedis);
            else return null;
        } finally {
            rpL1.putInstance(jedis);
        }
    }

    public interface IRedis<T> {
        T a(Jedis jedis);
    }

    public void quit() {
        if (rps != null)
            rps.quit();
        if (rpL1 != null)
            rpL1.quit();
    }
}