package com.zzq.dolls.redis.mini;

import com.zzq.dolls.redis.RedisMode;
import com.zzq.dolls.redis.RedisPool;
import redis.clients.jedis.*;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisMini implements Redis {

    private static final String DEF_SAMPLE_KEY = "def";

    protected RedisPool pool;

    protected RedisMode mode;

    public JedisMini(RedisPool pool, RedisMode mode) {
        this.pool = pool;
        this.mode = mode;
    }

    @Deprecated
    @Override
    public Object eval(String script, String sampleKey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.eval(script, sampleKey));
        } else {
            return pool.jedis(jedis -> jedis.eval(script));
        }
    }

    @Deprecated
    @Override
    public Object evalsha(String sha1, String sampleKey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.eval(sha1, sampleKey));
        } else {
            return pool.jedis(jedis -> jedis.eval(sha1));
        }
    }

    @Deprecated
    @Override
    public Boolean scriptExists(String sha1, String sampleKey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scriptExists(sha1, sampleKey));
        } else {
            return pool.jedis(jedis -> jedis.scriptExists(sha1));
        }
    }

    @Deprecated
    @Override
    public List<Boolean> scriptExists(String sampleKey, String... sha1) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scriptExists(sampleKey, sha1));
        } else {
            return pool.jedis(jedis -> jedis.scriptExists(sha1));
        }
    }

    @Deprecated
    @Override
    public String scriptLoad(String script, String sampleKey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scriptLoad(script, sampleKey));
        } else {
            return pool.jedis(jedis -> jedis.scriptLoad(script));
        }
    }

    @Deprecated
    @Override
    public String scriptFlush(String sampleKey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scriptFlush(sampleKey));
        } else {
            return pool.jedis(jedis -> jedis.scriptFlush());
        }
    }

    @Deprecated
    @Override
    public String scriptKill(String sampleKey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scriptKill(sampleKey));
        } else {
            return pool.jedis(jedis -> jedis.scriptKill());
        }
    }

    @Override
    public String set(String key, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.set(key, value));
        } else {
            return pool.jedis(jedis -> jedis.set(key, value));
        }
    }

    @Override
    public String set(String key, String value, SetParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.set(key, value, params));
        } else {
            return pool.jedis(jedis -> jedis.set(key, value, params));
        }
    }

    @Override
    public String get(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.get(key));
        } else {
            return pool.jedis(jedis -> jedis.get(key));
        }
    }

    @Override
    public Boolean exists(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.exists(key));
        } else {
            return pool.jedis(jedis -> jedis.exists(key));
        }
    }

    @Override
    public Long persist(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.persist(key));
        } else {
            return pool.jedis(jedis -> jedis.persist(key));
        }
    }

    @Override
    public String type(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.type(key));
        } else {
            return pool.jedis(jedis -> jedis.type(key));
        }
    }

    @Override
    public byte[] dump(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.dump(key));
        } else {
            return pool.jedis(jedis -> jedis.dump(key));
        }
    }

    @Override
    public String restore(String key, int ttl, byte[] serializedValue) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.restore(key, ttl, serializedValue));
        } else {
            return pool.jedis(jedis -> jedis.restore(key, ttl, serializedValue));
        }
    }

    @Override
    public Long expire(String key, int seconds) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.expire(key, seconds));
        } else {
            return pool.jedis(jedis -> jedis.expire(key, seconds));
        }
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pexpire(key, milliseconds));
        } else {
            return pool.jedis(jedis -> jedis.pexpire(key, milliseconds));
        }
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.expireAt(key, unixTime));
        } else {
            return pool.jedis(jedis -> jedis.expireAt(key, unixTime));
        }
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pexpireAt(key, millisecondsTimestamp));
        } else {
            return pool.jedis(jedis -> jedis.pexpireAt(key, millisecondsTimestamp));
        }
    }

    @Override
    public Long ttl(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.ttl(key));
        } else {
            return pool.jedis(jedis -> jedis.ttl(key));
        }
    }

    @Override
    public Long pttl(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pttl(key));
        } else {
            return pool.jedis(jedis -> jedis.pttl(key));
        }
    }

    @Override
    public Long touch(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.touch(key));
        } else {
            return pool.jedis(jedis -> jedis.touch(key));
        }
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.setbit(key, offset, value));
        } else {
            return pool.jedis(jedis -> jedis.setbit(key, offset, value));
        }
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.setbit(key, offset, value));
        } else {
            return pool.jedis(jedis -> jedis.setbit(key, offset, value));
        }
    }

    @Override
    public Boolean getbit(String key, long offset) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.getbit(key, offset));
        } else {
            return pool.jedis(jedis -> jedis.getbit(key, offset));
        }
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.setrange(key, offset, value));
        } else {
            return pool.jedis(jedis -> jedis.setrange(key, offset, value));
        }
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.getrange(key, startOffset, endOffset));
        } else {
            return pool.jedis(jedis -> jedis.getrange(key, startOffset, endOffset));
        }
    }

    @Override
    public String getSet(String key, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.getSet(key, value));
        } else {
            return pool.jedis(jedis -> jedis.getSet(key, value));
        }
    }

    @Override
    public Long setnx(String key, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.setnx(key, value));
        } else {
            return pool.jedis(jedis -> jedis.setnx(key, value));
        }
    }

    @Override
    public String setex(String key, int seconds, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.setex(key, seconds, value));
        } else {
            return pool.jedis(jedis -> jedis.setex(key, seconds, value));
        }
    }

    @Override
    public String psetex(String key, long milliseconds, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.psetex(key, milliseconds, value));
        } else {
            return pool.jedis(jedis -> jedis.psetex(key, milliseconds, value));
        }
    }

    @Override
    public Long decrBy(String key, long decrement) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.decrBy(key, decrement));
        } else {
            return pool.jedis(jedis -> jedis.decrBy(key, decrement));
        }
    }

    @Override
    public Long decr(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.decr(key));
        } else {
            return pool.jedis(jedis -> jedis.decr(key));
        }
    }

    @Override
    public Long incrBy(String key, long increment) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.incrBy(key, increment));
        } else {
            return pool.jedis(jedis -> jedis.incrBy(key, increment));
        }
    }

    @Override
    public Double incrByFloat(String key, double increment) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.incrByFloat(key, increment));
        } else {
            return pool.jedis(jedis -> jedis.incrByFloat(key, increment));
        }
    }

    @Override
    public Long incr(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.incr(key));
        } else {
            return pool.jedis(jedis -> jedis.incr(key));
        }
    }

    @Override
    public Long append(String key, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.append(key, value));
        } else {
            return pool.jedis(jedis -> jedis.append(key, value));
        }
    }

    @Override
    public String substr(String key, int start, int end) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.substr(key, start, end));
        } else {
            return pool.jedis(jedis -> jedis.substr(key, start, end));
        }
    }

    @Override
    public Long hset(String key, String field, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hset(key, field, value));
        } else {
            return pool.jedis(jedis -> jedis.hset(key, field, value));
        }
    }

    @Override
    public Long hset(String key, Map<String, String> hash) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hset(key, hash));
        } else {
            return pool.jedis(jedis -> jedis.hset(key, hash));
        }
    }

    @Override
    public String hget(String key, String field) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hget(key, field));
        } else {
            return pool.jedis(jedis -> jedis.hget(key, field));
        }
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hsetnx(key, field, value));
        } else {
            return pool.jedis(jedis -> jedis.hsetnx(key, field, value));
        }
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hmset(key, hash));
        } else {
            return pool.jedis(jedis -> jedis.hmset(key, hash));
        }
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hmget(key, fields));
        } else {
            return pool.jedis(jedis -> jedis.hmget(key, fields));
        }
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hincrBy(key, field, value));
        } else {
            return pool.jedis(jedis -> jedis.hincrBy(key, field, value));
        }
    }

    @Override
    public Boolean hexists(String key, String field) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hexists(key, field));
        } else {
            return pool.jedis(jedis -> jedis.hexists(key, field));
        }
    }

    @Override
    public Long hdel(String key, String... field) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hdel(key, field));
        } else {
            return pool.jedis(jedis -> jedis.hdel(key, field));
        }
    }

    @Override
    public Long hlen(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hlen(key));
        } else {
            return pool.jedis(jedis -> jedis.hlen(key));
        }
    }

    @Override
    public Set<String> hkeys(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hkeys(key));
        } else {
            return pool.jedis(jedis -> jedis.hkeys(key));
        }
    }

    @Override
    public List<String> hvals(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hvals(key));
        } else {
            return pool.jedis(jedis -> jedis.hvals(key));
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hgetAll(key));
        } else {
            return pool.jedis(jedis -> jedis.hgetAll(key));
        }
    }

    @Override
    public Long rpush(String key, String... string) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.rpush(key, string));
        } else {
            return pool.jedis(jedis -> jedis.rpush(key, string));
        }
    }

    @Override
    public Long lpush(String key, String... string) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lpush(key, string));
        } else {
            return pool.jedis(jedis -> jedis.lpush(key, string));
        }
    }

    @Override
    public Long llen(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.llen(key));
        } else {
            return pool.jedis(jedis -> jedis.llen(key));
        }
    }

    @Override
    public List<String> lrange(String key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lrange(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.lrange(key, start, stop));
        }
    }

    @Override
    public String ltrim(String key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.ltrim(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.ltrim(key, start, stop));
        }
    }

    @Override
    public String lindex(String key, long index) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lindex(key, index));
        } else {
            return pool.jedis(jedis -> jedis.lindex(key, index));
        }
    }

    @Override
    public String lset(String key, long index, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lset(key, index, value));
        } else {
            return pool.jedis(jedis -> jedis.lset(key, index, value));
        }
    }

    @Override
    public Long lrem(String key, long count, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lrem(key, count, value));
        } else {
            return pool.jedis(jedis -> jedis.lrem(key, count, value));
        }
    }

    @Override
    public String lpop(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lpop(key));
        } else {
            return pool.jedis(jedis -> jedis.lpop(key));
        }
    }

    @Override
    public String rpop(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.rpop(key));
        } else {
            return pool.jedis(jedis -> jedis.rpop(key));
        }
    }

    @Override
    public Long sadd(String key, String... member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sadd(key, member));
        } else {
            return pool.jedis(jedis -> jedis.sadd(key, member));
        }
    }

    @Override
    public Set<String> smembers(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.smembers(key));
        } else {
            return pool.jedis(jedis -> jedis.smembers(key));
        }
    }

    @Override
    public Long srem(String key, String... member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.srem(key, member));
        } else {
            return pool.jedis(jedis -> jedis.srem(key, member));
        }
    }

    @Override
    public String spop(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.spop(key));
        } else {
            return pool.jedis(jedis -> jedis.spop(key));
        }
    }

    @Override
    public Set<String> spop(String key, long count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.spop(key, count));
        } else {
            return pool.jedis(jedis -> jedis.spop(key, count));
        }
    }

    @Override
    public Long scard(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scard(key));
        } else {
            return pool.jedis(jedis -> jedis.scard(key));
        }
    }

    @Override
    public Boolean sismember(String key, String member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sismember(key, member));
        } else {
            return pool.jedis(jedis -> jedis.sismember(key, member));
        }
    }

    @Override
    public String srandmember(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.srandmember(key));
        } else {
            return pool.jedis(jedis -> jedis.srandmember(key));
        }
    }

    @Override
    public List<String> srandmember(String key, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.srandmember(key, count));
        } else {
            return pool.jedis(jedis -> jedis.srandmember(key, count));
        }
    }

    @Override
    public Long strlen(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.strlen(key));
        } else {
            return pool.jedis(jedis -> jedis.strlen(key));
        }
    }

    @Override
    public Long zadd(String key, double score, String member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zadd(key, score, member));
        } else {
            return pool.jedis(jedis -> jedis.zadd(key, score, member));
        }
    }

    @Override
    public Long zadd(String key, double score, String member, ZAddParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zadd(key, score, member, params));
        } else {
            return pool.jedis(jedis -> jedis.zadd(key, score, member, params));
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zadd(key, scoreMembers));
        } else {
            return pool.jedis(jedis -> jedis.zadd(key, scoreMembers));
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zadd(key, scoreMembers, params));
        } else {
            return pool.jedis(jedis -> jedis.zadd(key, scoreMembers, params));
        }
    }

    @Override
    public Set<String> zrange(String key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrange(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.zrange(key, start, stop));
        }
    }

    @Override
    public Long zrem(String key, String... members) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrem(key, members));
        } else {
            return pool.jedis(jedis -> jedis.zrem(key, members));
        }
    }

    @Override
    public Double zincrby(String key, double increment, String member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zincrby(key, increment, member));
        } else {
            return pool.jedis(jedis -> jedis.zincrby(key, increment, member));
        }
    }

    @Override
    public Double zincrby(String key, double increment, String member, ZIncrByParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zincrby(key, increment, member, params));
        } else {
            return pool.jedis(jedis -> jedis.zincrby(key, increment, member, params));
        }
    }

    @Override
    public Long zrank(String key, String member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrank(key, member));
        } else {
            return pool.jedis(jedis -> jedis.zrank(key, member));
        }
    }

    @Override
    public Long zrevrank(String key, String member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrank(key, member));
        } else {
            return pool.jedis(jedis -> jedis.zrevrank(key, member));
        }
    }

    @Override
    public Set<String> zrevrange(String key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrange(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.zrevrange(key, start, stop));
        }
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeWithScores(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.zrangeWithScores(key, start, stop));
        }
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeWithScores(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeWithScores(key, start, stop));
        }
    }

    @Override
    public Long zcard(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zcard(key));
        } else {
            return pool.jedis(jedis -> jedis.zcard(key));
        }
    }

    @Override
    public Double zscore(String key, String member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zscore(key, member));
        } else {
            return pool.jedis(jedis -> jedis.zscore(key, member));
        }
    }

    @Override
    public Tuple zpopmax(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zpopmax(key));
        } else {
            return pool.jedis(jedis -> jedis.zpopmax(key));
        }
    }

    @Override
    public Set<Tuple> zpopmax(String key, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zpopmax(key, count));
        } else {
            return pool.jedis(jedis -> jedis.zpopmax(key, count));
        }
    }

    @Override
    public Tuple zpopmin(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zpopmin(key));
        } else {
            return pool.jedis(jedis -> jedis.zpopmin(key));
        }
    }

    @Override
    public Set<Tuple> zpopmin(String key, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zpopmin(key, count));
        } else {
            return pool.jedis(jedis -> jedis.zpopmin(key, count));
        }
    }

    @Override
    public List<String> sort(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sort(key));
        } else {
            return pool.jedis(jedis -> jedis.sort(key));
        }
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sort(key, sortingParameters));
        } else {
            return pool.jedis(jedis -> jedis.sort(key, sortingParameters));
        }
    }

    @Override
    public Long zcount(String key, double min, double max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zcount(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zcount(key, min, max));
        }
    }

    @Override
    public Long zcount(String key, String min, String max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zcount(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zcount(key, min, max));
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScore(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScore(key, min, max));
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScore(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScore(key, min, max));
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScore(key, max, min));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScore(key, max, min));
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScore(key, min, max, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScore(key, min, max, offset, count));
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScore(key, max, min));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScore(key, max, min));
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScore(key, min, max, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScore(key, min, max, offset, count));
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScore(key, max, min, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScore(key, max, min, offset, count));
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScoreWithScores(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScoreWithScores(key, min, max));
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScoreWithScores(key, max, min));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScoreWithScores(key, max, min));
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScoreWithScores(key, min, max, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScoreWithScores(key, min, max, offset, count));
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScore(key, max, min, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScore(key, max, min, offset, count));
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScoreWithScores(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScoreWithScores(key, min, max));
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScoreWithScores(key, max, min));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScoreWithScores(key, max, min));
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScoreWithScores(key, min, max, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScoreWithScores(key, min, max, offset, count));
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScoreWithScores(key, max, min, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScoreWithScores(key, max, min, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
        }
    }

    @Override
    public Long zremrangeByRank(String key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zremrangeByRank(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.zremrangeByRank(key, start, stop));
        }
    }

    @Override
    public Long zremrangeByScore(String key, double min, double max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zremrangeByScore(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zremrangeByScore(key, min, max));
        }
    }

    @Override
    public Long zremrangeByScore(String key, String min, String max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zremrangeByScore(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zremrangeByScore(key, min, max));
        }
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zlexcount(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zlexcount(key, min, max));
        }
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByLex(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByLex(key, min, max));
        }
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByLex(key, min, max, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByLex(key, min, max, offset, count));
        }
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByLex(key, max, min));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByLex(key, max, min));
        }
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByLex(key, max, min, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByLex(key, max, min, offset, count));
        }
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zremrangeByLex(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zremrangeByLex(key, min, max));
        }
    }

    @Override
    public Long linsert(String key, ListPosition where, String pivot, String value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.linsert(key, where, pivot, value));
        } else {
            return pool.jedis(jedis -> jedis.linsert(key, where, pivot, value));
        }
    }

    @Override
    public Long lpushx(String key, String... string) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lpushx(key, string));
        } else {
            return pool.jedis(jedis -> jedis.lpushx(key, string));
        }
    }

    @Override
    public Long rpushx(String key, String... string) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.rpushx(key, string));
        } else {
            return pool.jedis(jedis -> jedis.rpushx(key, string));
        }
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.blpop(timeout, key));
        } else {
            return pool.jedis(jedis -> jedis.blpop(timeout, key));
        }
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.brpop(timeout, key));
        } else {
            return pool.jedis(jedis -> jedis.brpop(timeout, key));
        }
    }

    @Override
    public Long del(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.del(key));
        } else {
            return pool.jedis(jedis -> jedis.del(key));
        }
    }

    @Override
    public Long unlink(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.unlink(key));
        } else {
            return pool.jedis(jedis -> jedis.unlink(key));
        }
    }

    @Override
    public String echo(String string) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.echo(string));
        } else {
            return pool.jedis(jedis -> jedis.echo(string));
        }
    }

    @Override
    public Long bitcount(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.bitcount(key));
        } else {
            return pool.jedis(jedis -> jedis.bitcount(key));
        }
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.bitcount(key, start, end));
        } else {
            return pool.jedis(jedis -> jedis.bitcount(key, start, end));
        }
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hscan(key, cursor));
        } else {
            return pool.jedis(jedis -> jedis.hscan(key, cursor));
        }
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sscan(key, cursor));
        } else {
            return pool.jedis(jedis -> jedis.sscan(key, cursor));
        }
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zscan(key, cursor));
        } else {
            return pool.jedis(jedis -> jedis.zscan(key, cursor));
        }
    }

    @Override
    public Long pfadd(String key, String... elements) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pfadd(key, elements));
        } else {
            return pool.jedis(jedis -> jedis.pfadd(key, elements));
        }
    }

    @Override
    public long pfcount(String key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pfcount(key));
        } else {
            return pool.jedis(jedis -> jedis.pfcount(key));
        }
    }

    @Override
    public Long geoadd(String key, double longitude, double latitude, String member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geoadd(key, longitude, latitude, member));
        } else {
            return pool.jedis(jedis -> jedis.geoadd(key, longitude, latitude, member));
        }
    }

    @Override
    public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geoadd(key, memberCoordinateMap));
        } else {
            return pool.jedis(jedis -> jedis.geoadd(key, memberCoordinateMap));
        }
    }

    @Override
    public Double geodist(String key, String member1, String member2) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geodist(key, member1, member2));
        } else {
            return pool.jedis(jedis -> jedis.geodist(key, member1, member2));
        }
    }

    @Override
    public Double geodist(String key, String member1, String member2, GeoUnit unit) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geodist(key, member1, member2, unit));
        } else {
            return pool.jedis(jedis -> jedis.geodist(key, member1, member2, unit));
        }
    }

    @Override
    public List<String> geohash(String key, String... members) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geohash(key, members));
        } else {
            return pool.jedis(jedis -> jedis.geohash(key, members));
        }
    }

    @Override
    public List<GeoCoordinate> geopos(String key, String... members) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geopos(key, members));
        } else {
            return pool.jedis(jedis -> jedis.geopos(key, members));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadius(key, longitude, latitude, radius, unit));
        } else {
            return pool.jedis(jedis -> jedis.georadius(key, longitude, latitude, radius, unit));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusReadonly(key, longitude, latitude, radius, unit));
        } else {
            return pool.jedis(jedis -> jedis.georadiusReadonly(key, longitude, latitude, radius, unit));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadius(key, longitude, latitude, radius, unit, param));
        } else {
            return pool.jedis(jedis -> jedis.georadius(key, longitude, latitude, radius, unit, param));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusReadonly(key, longitude, latitude, radius, unit, param));
        } else {
            return pool.jedis(jedis -> jedis.georadiusReadonly(key, longitude, latitude, radius, unit, param));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusByMember(key, member, radius, unit));
        } else {
            return pool.jedis(jedis -> jedis.georadiusByMember(key, member, radius, unit));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusByMemberReadonly(key, member, radius, unit));
        } else {
            return pool.jedis(jedis -> jedis.georadiusByMemberReadonly(key, member, radius, unit));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusByMember(key, member, radius, unit, param));
        } else {
            return pool.jedis(jedis -> jedis.georadiusByMember(key, member, radius, unit, param));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusByMemberReadonly(key, member, radius, unit, param));
        } else {
            return pool.jedis(jedis -> jedis.georadiusByMemberReadonly(key, member, radius, unit, param));
        }
    }

    @Override
    public void close() throws IOException {
        pool.close();
    }

    @Override
    public String set(byte[] key, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.set(key, value));
        } else {
            return pool.jedis(jedis -> jedis.set(key, value));
        }
    }

    @Override
    public String set(byte[] key, byte[] value, SetParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.set(key, value, params));
        } else {
            return pool.jedis(jedis -> jedis.set(key, value, params));
        }
    }

    @Override
    public byte[] get(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.get(key));
        } else {
            return pool.jedis(jedis -> jedis.get(key));
        }
    }

    @Override
    public Boolean exists(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.exists(key));
        } else {
            return pool.jedis(jedis -> jedis.exists(key));
        }
    }

    @Override
    public Long persist(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.persist(key));
        } else {
            return pool.jedis(jedis -> jedis.persist(key));
        }
    }

    @Override
    public String type(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.type(key));
        } else {
            return pool.jedis(jedis -> jedis.type(key));
        }
    }

    @Override
    public byte[] dump(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.dump(key));
        } else {
            return pool.jedis(jedis -> jedis.dump(key));
        }
    }

    @Override
    public String restore(byte[] key, int ttl, byte[] serializedValue) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.restore(key, ttl, serializedValue));
        } else {
            return pool.jedis(jedis -> jedis.restore(key, ttl, serializedValue));
        }
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.expire(key, seconds));
        } else {
            return pool.jedis(jedis -> jedis.expire(key, seconds));
        }
    }

    @Override
    public Long pexpire(byte[] key, long milliseconds) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pexpire(key, milliseconds));
        } else {
            return pool.jedis(jedis -> jedis.pexpire(key, milliseconds));
        }
    }

    @Override
    public Long expireAt(byte[] key, long unixTime) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.expireAt(key, unixTime));
        } else {
            return pool.jedis(jedis -> jedis.expireAt(key, unixTime));
        }
    }

    @Override
    public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pexpireAt(key, millisecondsTimestamp));
        } else {
            return pool.jedis(jedis -> jedis.pexpireAt(key, millisecondsTimestamp));
        }
    }

    @Override
    public Long ttl(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.ttl(key));
        } else {
            return pool.jedis(jedis -> jedis.ttl(key));
        }
    }

    @Override
    public Long pttl(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pttl(key));
        } else {
            return pool.jedis(jedis -> jedis.pttl(key));
        }
    }

    @Override
    public Long touch(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.touch(key));
        } else {
            return pool.jedis(jedis -> jedis.touch(key));
        }
    }

    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.setbit(key, offset, value));
        } else {
            return pool.jedis(jedis -> jedis.setbit(key, offset, value));
        }
    }

    @Override
    public Boolean setbit(byte[] key, long offset, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.setbit(key, offset, value));
        } else {
            return pool.jedis(jedis -> jedis.setbit(key, offset, value));
        }
    }

    @Override
    public Boolean getbit(byte[] key, long offset) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.getbit(key, offset));
        } else {
            return pool.jedis(jedis -> jedis.getbit(key, offset));
        }
    }

    @Override
    public Long setrange(byte[] key, long offset, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.setrange(key, offset, value));
        } else {
            return pool.jedis(jedis -> jedis.setrange(key, offset, value));
        }
    }

    @Override
    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.getrange(key, startOffset, endOffset));
        } else {
            return pool.jedis(jedis -> jedis.getrange(key, startOffset, endOffset));
        }
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.getSet(key, value));
        } else {
            return pool.jedis(jedis -> jedis.getSet(key, value));
        }
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.setnx(key, value));
        } else {
            return pool.jedis(jedis -> jedis.setnx(key, value));
        }
    }

    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.setex(key, seconds, value));
        } else {
            return pool.jedis(jedis -> jedis.setex(key, seconds, value));
        }
    }

    @Override
    public String psetex(byte[] key, long milliseconds, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.psetex(key, milliseconds, value));
        } else {
            return pool.jedis(jedis -> jedis.psetex(key, milliseconds, value));
        }
    }

    @Override
    public Long decrBy(byte[] key, long decrement) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.decrBy(key, decrement));
        } else {
            return pool.jedis(jedis -> jedis.decrBy(key, decrement));
        }
    }

    @Override
    public Long decr(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.decr(key));
        } else {
            return pool.jedis(jedis -> jedis.decr(key));
        }
    }

    @Override
    public Long incrBy(byte[] key, long increment) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.incrBy(key, increment));
        } else {
            return pool.jedis(jedis -> jedis.incrBy(key, increment));
        }
    }

    @Override
    public Double incrByFloat(byte[] key, double increment) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.incrByFloat(key, increment));
        } else {
            return pool.jedis(jedis -> jedis.incrByFloat(key, increment));
        }
    }

    @Override
    public Long incr(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.incr(key));
        } else {
            return pool.jedis(jedis -> jedis.incr(key));
        }
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.append(key, value));
        } else {
            return pool.jedis(jedis -> jedis.append(key, value));
        }
    }

    @Override
    public byte[] substr(byte[] key, int start, int end) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.substr(key, start, end));
        } else {
            return pool.jedis(jedis -> jedis.substr(key, start, end));
        }
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hset(key, field, value));
        } else {
            return pool.jedis(jedis -> jedis.hset(key, field, value));
        }
    }

    @Override
    public Long hset(byte[] key, Map<byte[], byte[]> hash) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hset(key, hash));
        } else {
            return pool.jedis(jedis -> jedis.hset(key, hash));
        }
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hget(key, field));
        } else {
            return pool.jedis(jedis -> jedis.hget(key, field));
        }
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hsetnx(key, field, value));
        } else {
            return pool.jedis(jedis -> jedis.hsetnx(key, field, value));
        }
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hmset(key, hash));
        } else {
            return pool.jedis(jedis -> jedis.hmset(key, hash));
        }
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hmget(key, fields));
        } else {
            return pool.jedis(jedis -> jedis.hmget(key, fields));
        }
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hincrBy(key, field, value));
        } else {
            return pool.jedis(jedis -> jedis.hincrBy(key, field, value));
        }
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hincrByFloat(key, field, value));
        } else {
            return pool.jedis(jedis -> jedis.hincrByFloat(key, field, value));
        }
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hexists(key, field));
        } else {
            return pool.jedis(jedis -> jedis.hexists(key, field));
        }
    }

    @Override
    public Long hdel(byte[] key, byte[]... field) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hdel(key, field));
        } else {
            return pool.jedis(jedis -> jedis.hdel(key, field));
        }
    }

    @Override
    public Long hlen(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hlen(key));
        } else {
            return pool.jedis(jedis -> jedis.hlen(key));
        }
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hkeys(key));
        } else {
            return pool.jedis(jedis -> jedis.hkeys(key));
        }
    }

    @Override
    public List<byte[]> hvals(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hvals(key));
        } else {
            return pool.jedis(jedis -> jedis.hvals(key));
        }
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hgetAll(key));
        } else {
            return pool.jedis(jedis -> jedis.hgetAll(key));
        }
    }

    @Override
    public Long rpush(byte[] key, byte[]... args) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.rpush(key, args));
        } else {
            return pool.jedis(jedis -> jedis.rpush(key, args));
        }
    }

    @Override
    public Long lpush(byte[] key, byte[]... args) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lpush(key, args));
        } else {
            return pool.jedis(jedis -> jedis.lpush(key, args));
        }
    }

    @Override
    public Long llen(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.llen(key));
        } else {
            return pool.jedis(jedis -> jedis.llen(key));
        }
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lrange(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.lrange(key, start, stop));
        }
    }

    @Override
    public String ltrim(byte[] key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.ltrim(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.ltrim(key, start, stop));
        }
    }

    @Override
    public byte[] lindex(byte[] key, long index) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lindex(key, index));
        } else {
            return pool.jedis(jedis -> jedis.lindex(key, index));
        }
    }

    @Override
    public String lset(byte[] key, long index, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lset(key, index, value));
        } else {
            return pool.jedis(jedis -> jedis.lset(key, index, value));
        }
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lrem(key, count, value));
        } else {
            return pool.jedis(jedis -> jedis.lrem(key, count, value));
        }
    }

    @Override
    public byte[] lpop(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lpop(key));
        } else {
            return pool.jedis(jedis -> jedis.lpop(key));
        }
    }

    @Override
    public byte[] rpop(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.rpop(key));
        } else {
            return pool.jedis(jedis -> jedis.rpop(key));
        }
    }

    @Override
    public Long sadd(byte[] key, byte[]... member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sadd(key, member));
        } else {
            return pool.jedis(jedis -> jedis.sadd(key, member));
        }
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.smembers(key));
        } else {
            return pool.jedis(jedis -> jedis.smembers(key));
        }
    }

    @Override
    public Long srem(byte[] key, byte[]... member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.srem(key, member));
        } else {
            return pool.jedis(jedis -> jedis.srem(key, member));
        }
    }

    @Override
    public byte[] spop(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.spop(key));
        } else {
            return pool.jedis(jedis -> jedis.spop(key));
        }
    }

    @Override
    public Set<byte[]> spop(byte[] key, long count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.spop(key, count));
        } else {
            return pool.jedis(jedis -> jedis.spop(key, count));
        }
    }

    @Override
    public Long scard(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scard(key));
        } else {
            return pool.jedis(jedis -> jedis.scard(key));
        }
    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sismember(key, member));
        } else {
            return pool.jedis(jedis -> jedis.sismember(key, member));
        }
    }

    @Override
    public byte[] srandmember(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.srandmember(key));
        } else {
            return pool.jedis(jedis -> jedis.srandmember(key));
        }
    }

    @Override
    public List<byte[]> srandmember(byte[] key, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.srandmember(key, count));
        } else {
            return pool.jedis(jedis -> jedis.srandmember(key, count));
        }
    }

    @Override
    public Long strlen(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.strlen(key));
        } else {
            return pool.jedis(jedis -> jedis.strlen(key));
        }
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zadd(key, score, member));
        } else {
            return pool.jedis(jedis -> jedis.zadd(key, score, member));
        }
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member, ZAddParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zadd(key, score, member, params));
        } else {
            return pool.jedis(jedis -> jedis.zadd(key, score, member, params));
        }
    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zadd(key, scoreMembers));
        } else {
            return pool.jedis(jedis -> jedis.zadd(key, scoreMembers));
        }
    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers, ZAddParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zadd(key, scoreMembers, params));
        } else {
            return pool.jedis(jedis -> jedis.zadd(key, scoreMembers, params));
        }
    }

    @Override
    public Set<byte[]> zrange(byte[] key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrange(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.zrange(key, start, stop));
        }
    }

    @Override
    public Long zrem(byte[] key, byte[]... members) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrem(key, members));
        } else {
            return pool.jedis(jedis -> jedis.zrem(key, members));
        }
    }

    @Override
    public Double zincrby(byte[] key, double increment, byte[] member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zincrby(key, increment, member));
        } else {
            return pool.jedis(jedis -> jedis.zincrby(key, increment, member));
        }
    }

    @Override
    public Double zincrby(byte[] key, double increment, byte[] member, ZIncrByParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zincrby(key, increment, member, params));
        } else {
            return pool.jedis(jedis -> jedis.zincrby(key, increment, member, params));
        }
    }

    @Override
    public Long zrank(byte[] key, byte[] member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrank(key, member));
        } else {
            return pool.jedis(jedis -> jedis.zrank(key, member));
        }
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrank(key, member));
        } else {
            return pool.jedis(jedis -> jedis.zrevrank(key, member));
        }
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrange(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.zrevrange(key, start, stop));
        }
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeWithScores(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.zrangeWithScores(key, start, stop));
        }
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeWithScores(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeWithScores(key, start, stop));
        }
    }

    @Override
    public Long zcard(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zcard(key));
        } else {
            return pool.jedis(jedis -> jedis.zcard(key));
        }
    }

    @Override
    public Double zscore(byte[] key, byte[] member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zscore(key, member));
        } else {
            return pool.jedis(jedis -> jedis.zscore(key, member));
        }
    }

    @Override
    public Tuple zpopmax(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zpopmax(key));
        } else {
            return pool.jedis(jedis -> jedis.zpopmax(key));
        }
    }

    @Override
    public Set<Tuple> zpopmax(byte[] key, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zpopmax(key, count));
        } else {
            return pool.jedis(jedis -> jedis.zpopmax(key, count));
        }
    }

    @Override
    public Tuple zpopmin(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zpopmin(key));
        } else {
            return pool.jedis(jedis -> jedis.zpopmin(key));
        }
    }

    @Override
    public Set<Tuple> zpopmin(byte[] key, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zpopmin(key, count));
        } else {
            return pool.jedis(jedis -> jedis.zpopmin(key, count));
        }
    }

    @Override
    public List<byte[]> sort(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sort(key));
        } else {
            return pool.jedis(jedis -> jedis.sort(key));
        }
    }

    @Override
    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sort(key, sortingParameters));
        } else {
            return pool.jedis(jedis -> jedis.sort(key, sortingParameters));
        }
    }

    @Override
    public Long zcount(byte[] key, double min, double max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zcount(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zcount(key, min, max));
        }
    }

    @Override
    public Long zcount(byte[] key, byte[] min, byte[] max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zcount(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zcount(key, min, max));
        }
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScore(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScore(key, min, max));
        }
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScore(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScore(key, min, max));
        }
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScore(key, max, min));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScore(key, max, min));
        }
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScore(key, min, max, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScore(key, min, max, offset, count));
        }
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScore(key, max, min));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScore(key, max, min));
        }
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScore(key, min, max, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScore(key, min, max, offset, count));
        }
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScore(key, max, min, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScore(key, max, min, offset, count));
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScoreWithScores(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScoreWithScores(key, min, max));
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScoreWithScores(key, max, min));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScoreWithScores(key, max, min));
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScoreWithScores(key, min, max, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScoreWithScores(key, min, max, offset, count));
        }
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScore(key, max, min, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScore(key, max, min, offset, count));
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScoreWithScores(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScoreWithScores(key, min, max));
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScoreWithScores(key, max, min));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScoreWithScores(key, max, min));
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByScoreWithScores(key, min, max, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByScoreWithScores(key, min, max, offset, count));
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScoreWithScores(key, max, min, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByScoreWithScores(key, max, min, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
        }
    }

    @Override
    public Long zremrangeByRank(byte[] key, long start, long stop) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zremrangeByRank(key, start, stop));
        } else {
            return pool.jedis(jedis -> jedis.zremrangeByRank(key, start, stop));
        }
    }

    @Override
    public Long zremrangeByScore(byte[] key, double min, double max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zremrangeByScore(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zremrangeByScore(key, min, max));
        }
    }

    @Override
    public Long zremrangeByScore(byte[] key, byte[] min, byte[] max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zremrangeByScore(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zremrangeByScore(key, min, max));
        }
    }

    @Override
    public Long zlexcount(byte[] key, byte[] min, byte[] max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zlexcount(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zlexcount(key, min, max));
        }
    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByLex(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByLex(key, min, max));
        }
    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrangeByLex(key, min, max, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrangeByLex(key, min, max, offset, count));
        }
    }

    @Override
    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByLex(key, max, min));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByLex(key, max, min));
        }
    }

    @Override
    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zrevrangeByLex(key, max, min, offset, count));
        } else {
            return pool.jedis(jedis -> jedis.zrevrangeByLex(key, max, min, offset, count));
        }
    }

    @Override
    public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zremrangeByLex(key, min, max));
        } else {
            return pool.jedis(jedis -> jedis.zremrangeByLex(key, min, max));
        }
    }

    @Override
    public Long linsert(byte[] key, ListPosition where, byte[] pivot, byte[] value) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.linsert(key, where, pivot, value));
        } else {
            return pool.jedis(jedis -> jedis.linsert(key, where, pivot, value));
        }
    }

    @Override
    public Long lpushx(byte[] key, byte[]... arg) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.lpushx(key, arg));
        } else {
            return pool.jedis(jedis -> jedis.lpushx(key, arg));
        }
    }

    @Override
    public Long rpushx(byte[] key, byte[]... arg) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.rpushx(key, arg));
        } else {
            return pool.jedis(jedis -> jedis.rpushx(key, arg));
        }
    }

    @Override
    public Long del(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.del(key));
        } else {
            return pool.jedis(jedis -> jedis.del(key));
        }
    }

    @Override
    public Long unlink(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.unlink(key));
        } else {
            return pool.jedis(jedis -> jedis.unlink(key));
        }
    }

    @Override
    public byte[] echo(byte[] arg) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.echo(arg));
        } else {
            return pool.jedis(jedis -> jedis.echo(arg));
        }
    }

    @Override
    public Long bitcount(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.bitcount(key));
        } else {
            return pool.jedis(jedis -> jedis.bitcount(key));
        }
    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.bitcount(key, start, end));
        } else {
            return pool.jedis(jedis -> jedis.bitcount(key, start, end));
        }
    }

    @Override
    public Long pfadd(byte[] key, byte[]... elements) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pfadd(key, elements));
        } else {
            return pool.jedis(jedis -> jedis.pfadd(key, elements));
        }
    }

    @Override
    public long pfcount(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pfcount(key));
        } else {
            return pool.jedis(jedis -> jedis.pfcount(key));
        }
    }

    @Override
    public Long geoadd(byte[] key, double longitude, double latitude, byte[] member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geoadd(key, longitude, latitude, member));
        } else {
            return pool.jedis(jedis -> jedis.geoadd(key, longitude, latitude, member));
        }
    }

    @Override
    public Long geoadd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geoadd(key, memberCoordinateMap));
        } else {
            return pool.jedis(jedis -> jedis.geoadd(key, memberCoordinateMap));
        }
    }

    @Override
    public Double geodist(byte[] key, byte[] member1, byte[] member2) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geodist(key, member1, member2));
        } else {
            return pool.jedis(jedis -> jedis.geodist(key, member1, member2));
        }
    }

    @Override
    public Double geodist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geodist(key, member1, member2, unit));
        } else {
            return pool.jedis(jedis -> jedis.geodist(key, member1, member2, unit));
        }
    }

    @Override
    public List<byte[]> geohash(byte[] key, byte[]... members) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geohash(key, members));
        } else {
            return pool.jedis(jedis -> jedis.geohash(key, members));
        }
    }

    @Override
    public List<GeoCoordinate> geopos(byte[] key, byte[]... members) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.geopos(key, members));
        } else {
            return pool.jedis(jedis -> jedis.geopos(key, members));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude, double radius, GeoUnit unit) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadius(key, longitude, latitude, radius, unit));
        } else {
            return pool.jedis(jedis -> jedis.georadius(key, longitude, latitude, radius, unit));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(byte[] key, double longitude, double latitude, double radius, GeoUnit unit) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusReadonly(key, longitude, latitude, radius, unit));
        } else {
            return pool.jedis(jedis -> jedis.georadiusReadonly(key, longitude, latitude, radius, unit));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadius(key, longitude, latitude, radius, unit, param));
        } else {
            return pool.jedis(jedis -> jedis.georadius(key, longitude, latitude, radius, unit, param));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(byte[] key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusReadonly(key, longitude, latitude, radius, unit, param));
        } else {
            return pool.jedis(jedis -> jedis.georadiusReadonly(key, longitude, latitude, radius, unit, param));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusByMember(key, member, radius, unit));
        } else {
            return pool.jedis(jedis -> jedis.georadiusByMember(key, member, radius, unit));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(byte[] key, byte[] member, double radius, GeoUnit unit) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusByMemberReadonly(key, member, radius, unit));
        } else {
            return pool.jedis(jedis -> jedis.georadiusByMemberReadonly(key, member, radius, unit));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit, GeoRadiusParam param) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusByMember(key, member, radius, unit, param));
        } else {
            return pool.jedis(jedis -> jedis.georadiusByMember(key, member, radius, unit, param));
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(byte[] key, byte[] member, double radius, GeoUnit unit, GeoRadiusParam param) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.georadiusByMemberReadonly(key, member, radius, unit, param));
        } else {
            return pool.jedis(jedis -> jedis.georadiusByMemberReadonly(key, member, radius, unit, param));
        }
    }

    @Override
    public ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hscan(key, cursor));
        } else {
            return pool.jedis(jedis -> jedis.hscan(key, cursor));
        }
    }

    @Override
    public ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hscan(key, cursor, params));
        } else {
            return pool.jedis(jedis -> jedis.hscan(key, cursor, params));
        }
    }

    @Override
    public ScanResult<byte[]> sscan(byte[] key, byte[] cursor) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sscan(key, cursor));
        } else {
            return pool.jedis(jedis -> jedis.sscan(key, cursor));
        }
    }

    @Override
    public ScanResult<byte[]> sscan(byte[] key, byte[] cursor, ScanParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sscan(key, cursor, params));
        } else {
            return pool.jedis(jedis -> jedis.sscan(key, cursor, params));
        }
    }

    @Override
    public ScanResult<Tuple> zscan(byte[] key, byte[] cursor) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zscan(key, cursor));
        } else {
            return pool.jedis(jedis -> jedis.zscan(key, cursor));
        }
    }

    @Override
    public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zscan(key, cursor, params));
        } else {
            return pool.jedis(jedis -> jedis.zscan(key, cursor, params));
        }
    }

    @Override
    public List<Long> bitfield(byte[] key, byte[]... arguments) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.bitfield(key, arguments));
        } else {
            return pool.jedis(jedis -> jedis.bitfield(key, arguments));
        }
    }

    @Override
    public List<Long> bitfieldReadonly(byte[] key, byte[]... arguments) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.bitfieldReadonly(key, arguments));
        } else {
            return pool.jedis(jedis -> jedis.bitfieldReadonly(key, arguments));
        }
    }

    @Override
    public Long hstrlen(byte[] key, byte[] field) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.hstrlen(key, field));
        } else {
            return pool.jedis(jedis -> jedis.hstrlen(key, field));
        }
    }

    @Override
    public byte[] xadd(byte[] key, byte[] id, Map<byte[], byte[]> hash, long maxLen, boolean approximateLength) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xadd(key, id, hash, maxLen, approximateLength));
        } else {
            return pool.jedis(jedis -> jedis.xadd(key, id, hash, maxLen, approximateLength));
        }
    }

    @Override
    public Long xlen(byte[] key) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xlen(key));
        } else {
            return pool.jedis(jedis -> jedis.xlen(key));
        }
    }

    @Override
    public List<byte[]> xrange(byte[] key, byte[] start, byte[] end, long count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xrange(key, start, end, count));
        } else {
            return pool.jedis(jedis -> jedis.xrange(key, start, end, count));
        }
    }

    @Override
    public List<byte[]> xrevrange(byte[] key, byte[] end, byte[] start, int count) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xrevrange(key, end, start, count));
        } else {
            return pool.jedis(jedis -> jedis.xrevrange(key, end, start, count));
        }
    }

    @Override
    public Long xack(byte[] key, byte[] group, byte[]... ids) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xack(key, group, ids));
        } else {
            return pool.jedis(jedis -> jedis.xack(key, group, ids));
        }
    }

    @Override
    public String xgroupCreate(byte[] key, byte[] consumer, byte[] id, boolean makeStream) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xgroupCreate(key, consumer, id, makeStream));
        } else {
            return pool.jedis(jedis -> jedis.xgroupCreate(key, consumer, id, makeStream));
        }
    }

    @Override
    public String xgroupSetID(byte[] key, byte[] consumer, byte[] id) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xgroupSetID(key, consumer, id));
        } else {
            return pool.jedis(jedis -> jedis.xgroupSetID(key, consumer, id));
        }
    }

    @Override
    public Long xgroupDestroy(byte[] key, byte[] consumer) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xgroupDestroy(key, consumer));
        } else {
            return pool.jedis(jedis -> jedis.xgroupDestroy(key, consumer));
        }
    }

    @Override
    public Long xgroupDelConsumer(byte[] key, byte[] consumer, byte[] consumerName) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xgroupDelConsumer(key, consumer, consumerName));
        } else {
            return pool.jedis(jedis -> jedis.xgroupDelConsumer(key, consumer, consumerName));
        }
    }

    @Override
    public Long xdel(byte[] key, byte[]... ids) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xdel(key, ids));
        } else {
            return pool.jedis(jedis -> jedis.xdel(key, ids));
        }
    }

    @Override
    public Long xtrim(byte[] key, long maxLen, boolean approximateLength) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xtrim(key, maxLen, approximateLength));
        } else {
            return pool.jedis(jedis -> jedis.xtrim(key, maxLen, approximateLength));
        }
    }

    @Override
    public List<byte[]> xpending(byte[] key, byte[] groupname, byte[] start, byte[] end, int count, byte[] consumername) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xpending(key, groupname, start, end, count, consumername));
        } else {
            return pool.jedis(jedis -> jedis.xpending(key, groupname, start, end, count, consumername));
        }
    }

    @Override
    public List<byte[]> xclaim(byte[] key, byte[] groupname, byte[] consumername, long minIdleTime, long newIdleTime, int retries, boolean force, byte[][] ids) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xclaim(key, groupname, consumername, minIdleTime, newIdleTime, retries, force, ids));
        } else {
            return pool.jedis(jedis -> jedis.xclaim(key, groupname, consumername, minIdleTime, newIdleTime, retries, force, ids));
        }
    }

    @Override
    public Long del(byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.del(keys));
        } else {
            return pool.jedis(jedis -> jedis.del(keys));
        }
    }

    @Override
    public Long unlink(byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.unlink(keys));
        } else {
            return pool.jedis(jedis -> jedis.unlink(keys));
        }
    }

    @Override
    public Long exists(byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.exists(keys));
        } else {
            return pool.jedis(jedis -> jedis.exists(keys));
        }
    }

    @Override
    public List<byte[]> blpop(int timeout, byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.blpop(timeout, keys));
        } else {
            return pool.jedis(jedis -> jedis.blpop(timeout, keys));
        }
    }

    @Override
    public List<byte[]> brpop(int timeout, byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.brpop(timeout, keys));
        } else {
            return pool.jedis(jedis -> jedis.brpop(timeout, keys));
        }
    }

    @Override
    public List<byte[]> mget(byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.mget(keys));
        } else {
            return pool.jedis(jedis -> jedis.mget(keys));
        }
    }

    @Override
    public String mset(byte[]... keysvalues) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.mset(keysvalues));
        } else {
            return pool.jedis(jedis -> jedis.mset(keysvalues));
        }
    }

    @Override
    public Long msetnx(byte[]... keysvalues) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.msetnx(keysvalues));
        } else {
            return pool.jedis(jedis -> jedis.msetnx(keysvalues));
        }
    }

    @Override
    public String rename(byte[] oldkey, byte[] newkey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.rename(oldkey, newkey));
        } else {
            return pool.jedis(jedis -> jedis.rename(oldkey, newkey));
        }
    }

    @Override
    public Long renamenx(byte[] oldkey, byte[] newkey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.renamenx(oldkey, newkey));
        } else {
            return pool.jedis(jedis -> jedis.renamenx(oldkey, newkey));
        }
    }

    @Override
    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.rpoplpush(srckey, dstkey));
        } else {
            return pool.jedis(jedis -> jedis.rpoplpush(srckey, dstkey));
        }
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sdiff(keys));
        } else {
            return pool.jedis(jedis -> jedis.sdiff(keys));
        }
    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sdiffstore(dstkey, keys));
        } else {
            return pool.jedis(jedis -> jedis.sdiffstore(dstkey, keys));
        }
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sinter(keys));
        } else {
            return pool.jedis(jedis -> jedis.sinter(keys));
        }
    }

    @Override
    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sinterstore(dstkey, keys));
        } else {
            return pool.jedis(jedis -> jedis.sinterstore(dstkey, keys));
        }
    }

    @Override
    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.smove(srckey, dstkey, member));
        } else {
            return pool.jedis(jedis -> jedis.smove(srckey, dstkey, member));
        }
    }

    @Override
    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sort(key, sortingParameters, dstkey));
        } else {
            return pool.jedis(jedis -> jedis.sort(key, sortingParameters, dstkey));
        }
    }

    @Override
    public Long sort(byte[] key, byte[] dstkey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sort(key, dstkey));
        } else {
            return pool.jedis(jedis -> jedis.sort(key, dstkey));
        }
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sunion(keys));
        } else {
            return pool.jedis(jedis -> jedis.sunion(keys));
        }
    }

    @Override
    public Long sunionstore(byte[] dstkey, byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sunionstore(dstkey, keys));
        } else {
            return pool.jedis(jedis -> jedis.sunionstore(dstkey, keys));
        }
    }

    @Override
    public Long zinterstore(byte[] dstkey, byte[]... sets) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zinterstore(dstkey, sets));
        } else {
            return pool.jedis(jedis -> jedis.zinterstore(dstkey, sets));
        }
    }

    @Override
    public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zinterstore(dstkey, params, sets));
        } else {
            return pool.jedis(jedis -> jedis.zinterstore(dstkey, params, sets));
        }
    }

    @Override
    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zunionstore(dstkey, sets));
        } else {
            return pool.jedis(jedis -> jedis.zunionstore(dstkey, sets));
        }
    }

    @Override
    public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zunionstore(dstkey, params, sets));
        } else {
            return pool.jedis(jedis -> jedis.zunionstore(dstkey, params, sets));
        }
    }

    @Override
    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.brpoplpush(source, destination, timeout));
        } else {
            return pool.jedis(jedis -> jedis.brpoplpush(source, destination, timeout));
        }
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.publish(channel, message));
        } else {
            return pool.jedis(jedis -> jedis.publish(channel, message));
        }
    }

    @Override
    public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
        pool.jedis(jedis -> {
            jedis.subscribe(jedisPubSub, channels);
            return null;
        });

    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
        pool.jedis(jedis -> {
            jedis.psubscribe(jedisPubSub, patterns);
            return null;
        });

    }

    @Override
    public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.bitop(op, destKey, srcKeys));
        } else {
            return pool.jedis(jedis -> jedis.bitop(op, destKey, srcKeys));
        }
    }

    @Override
    public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pfmerge(destkey, sourcekeys));
        } else {
            return pool.jedis(jedis -> jedis.pfmerge(destkey, sourcekeys));
        }
    }

    @Override
    public Long pfcount(byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pfcount(keys));
        } else {
            return pool.jedis(jedis -> jedis.pfcount(keys));
        }
    }

    @Override
    public Long touch(byte[]... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.touch(keys));
        } else {
            return pool.jedis(jedis -> jedis.touch(keys));
        }
    }

    @Override
    public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scan(cursor, params));
        } else {
            return pool.jedis(jedis -> jedis.scan(cursor, params));
        }
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.keys(pattern));
        } else {
            return pool.jedis(jedis -> jedis.keys(pattern));
        }
    }

    @Override
    public List<byte[]> xread(int count, long block, Map<byte[], byte[]> streams) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xread(count, block, streams));
        } else {
            return pool.jedis(jedis -> jedis.xread(count, block, streams));
        }
    }

    @Override
    public List<byte[]> xreadGroup(byte[] groupname, byte[] consumer, int count, long block, boolean noAck, Map<byte[], byte[]> streams) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xreadGroup(groupname, consumer, count, block, noAck, streams));
        } else {
            return pool.jedis(jedis -> jedis.xreadGroup(groupname, consumer, count, block, noAck, streams));
        }
    }

    @Override
    public Long del(String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.del(keys));
        } else {
            return pool.jedis(jedis -> jedis.del(keys));
        }
    }

    @Override
    public Long unlink(String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.unlink(keys));
        } else {
            return pool.jedis(jedis -> jedis.unlink(keys));
        }
    }

    @Override
    public Long exists(String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.exists(keys));
        } else {
            return pool.jedis(jedis -> jedis.exists(keys));
        }
    }

    @Override
    public List<String> blpop(int timeout, String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.blpop(timeout, keys));
        } else {
            return pool.jedis(jedis -> jedis.blpop(timeout, keys));
        }
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.brpop(timeout, keys));
        } else {
            return pool.jedis(jedis -> jedis.brpop(timeout, keys));
        }
    }

    @Override
    public Set<String> keys(String pattern) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.keys(pattern));
        } else {
            return pool.jedis(jedis -> jedis.keys(pattern));
        }
    }

    @Override
    public List<String> mget(String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.mget(keys));
        } else {
            return pool.jedis(jedis -> jedis.mget(keys));
        }
    }

    @Override
    public String mset(String... keysvalues) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.mset(keysvalues));
        } else {
            return pool.jedis(jedis -> jedis.mset(keysvalues));
        }
    }

    @Override
    public Long msetnx(String... keysvalues) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.msetnx(keysvalues));
        } else {
            return pool.jedis(jedis -> jedis.msetnx(keysvalues));
        }
    }

    @Override
    public String rename(String oldkey, String newkey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.rename(oldkey, newkey));
        } else {
            return pool.jedis(jedis -> jedis.rename(oldkey, newkey));
        }
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.renamenx(oldkey, newkey));
        } else {
            return pool.jedis(jedis -> jedis.renamenx(oldkey, newkey));
        }
    }

    @Override
    public String rpoplpush(String srckey, String dstkey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.rpoplpush(srckey, dstkey));
        } else {
            return pool.jedis(jedis -> jedis.rpoplpush(srckey, dstkey));
        }
    }

    @Override
    public Set<String> sdiff(String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sdiff(keys));
        } else {
            return pool.jedis(jedis -> jedis.sdiff(keys));
        }
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sdiffstore(dstkey, keys));
        } else {
            return pool.jedis(jedis -> jedis.sdiffstore(dstkey, keys));
        }
    }

    @Override
    public Set<String> sinter(String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sinter(keys));
        } else {
            return pool.jedis(jedis -> jedis.sinter(keys));
        }
    }

    @Override
    public Long sinterstore(String dstkey, String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sinterstore(dstkey, keys));
        } else {
            return pool.jedis(jedis -> jedis.sinterstore(dstkey, keys));
        }
    }

    @Override
    public Long smove(String srckey, String dstkey, String member) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.smove(srckey, dstkey, member));
        } else {
            return pool.jedis(jedis -> jedis.smove(srckey, dstkey, member));
        }
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sort(key, sortingParameters, dstkey));
        } else {
            return pool.jedis(jedis -> jedis.sort(key, sortingParameters, dstkey));
        }
    }

    @Override
    public Long sort(String key, String dstkey) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sort(key, dstkey));
        } else {
            return pool.jedis(jedis -> jedis.sort(key, dstkey));
        }
    }

    @Override
    public Set<String> sunion(String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sunion(keys));
        } else {
            return pool.jedis(jedis -> jedis.sunion(keys));
        }
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.sunionstore(dstkey, keys));
        } else {
            return pool.jedis(jedis -> jedis.sunionstore(dstkey, keys));
        }
    }

    @Override
    public Long zinterstore(String dstkey, String... sets) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zinterstore(dstkey, sets));
        } else {
            return pool.jedis(jedis -> jedis.zinterstore(dstkey, sets));
        }
    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zinterstore(dstkey, params, sets));
        } else {
            return pool.jedis(jedis -> jedis.zinterstore(dstkey, params, sets));
        }
    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zunionstore(dstkey, sets));
        } else {
            return pool.jedis(jedis -> jedis.zunionstore(dstkey, sets));
        }
    }

    @Override
    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.zunionstore(dstkey, params, sets));
        } else {
            return pool.jedis(jedis -> jedis.zunionstore(dstkey, params, sets));
        }
    }

    @Override
    public String brpoplpush(String source, String destination, int timeout) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.brpoplpush(source, destination, timeout));
        } else {
            return pool.jedis(jedis -> jedis.brpoplpush(source, destination, timeout));
        }
    }

    @Override
    public Long publish(String channel, String message) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.publish(channel, message));
        } else {
            return pool.jedis(jedis -> jedis.publish(channel, message));
        }
    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        pool.jedis(jedis -> {
            jedis.subscribe(jedisPubSub, channels);
            return null;
        });

    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        pool.jedis(jedis -> {
            jedis.psubscribe(jedisPubSub, patterns);
            return null;
        });

    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.bitop(op, destKey, srcKeys));
        } else {
            return pool.jedis(jedis -> jedis.bitop(op, destKey, srcKeys));
        }
    }

    @Override
    public ScanResult<String> scan(String cursor, ScanParams params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scan(cursor, params));
        } else {
            return pool.jedis(jedis -> jedis.scan(cursor, params));
        }
    }

    @Override
    public String pfmerge(String destkey, String... sourcekeys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pfmerge(destkey, sourcekeys));
        } else {
            return pool.jedis(jedis -> jedis.pfmerge(destkey, sourcekeys));
        }
    }

    @Override
    public long pfcount(String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.pfcount(keys));
        } else {
            return pool.jedis(jedis -> jedis.pfcount(keys));
        }
    }

    @Override
    public Long touch(String... keys) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.touch(keys));
        } else {
            return pool.jedis(jedis -> jedis.touch(keys));
        }
    }

    @SafeVarargs
    @Override
    public final List<Map.Entry<String, List<StreamEntry>>> xread(int count, long block, Map.Entry<String, StreamEntryID>... streams) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xread(count, block, streams));
        } else {
            return pool.jedis(jedis -> jedis.xread(count, block, streams));
        }
    }

    @SafeVarargs
    @Override
    public final List<Map.Entry<String, List<StreamEntry>>> xreadGroup(String groupname, String consumer, int count, long block, boolean noAck, Map.Entry<String, StreamEntryID>... streams) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.xreadGroup(groupname, consumer, count, block, noAck, streams));
        } else {
            return pool.jedis(jedis -> jedis.xreadGroup(groupname, consumer, count, block, noAck, streams));
        }
    }

    @Override
    public Object eval(String script, int keyCount, String... params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.eval(script, keyCount, params));
        } else {
            return pool.jedis(jedis -> jedis.eval(script, keyCount, params));
        }
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.eval(script, keys, args));
        } else {
            return pool.jedis(jedis -> jedis.eval(script, keys, args));
        }
    }

    @Override
    public Object eval(String script) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.eval(script, DEF_SAMPLE_KEY));
        } else {
            return pool.jedis(jedis -> jedis.eval(script));
        }
    }

    @Override
    public Object evalsha(String sha1) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.evalsha(sha1, DEF_SAMPLE_KEY));
        } else {
            return pool.jedis(jedis -> jedis.evalsha(sha1));
        }
    }

    @Override
    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.evalsha(sha1, keys, args));
        } else {
            return pool.jedis(jedis -> jedis.evalsha(sha1, keys, args));
        }
    }

    @Override
    public Object evalsha(String sha1, int keyCount, String... params) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.evalsha(sha1, keyCount, params));
        } else {
            return pool.jedis(jedis -> jedis.evalsha(sha1, keyCount, params));
        }
    }

    @Override
    public Boolean scriptExists(String sha1) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scriptExists(sha1, DEF_SAMPLE_KEY));
        } else {
            return pool.jedis(jedis -> jedis.scriptExists(sha1));
        }
    }

    @Override
    public List<Boolean> scriptExists(String... sha1) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scriptExists(DEF_SAMPLE_KEY, sha1));
        } else {
            return pool.jedis(jedis -> jedis.scriptExists(sha1));
        }
    }

    @Override
    public String scriptLoad(String script) {
        if (mode == RedisMode.CLUSTER) {
            return pool.cluster(cluster -> cluster.scriptLoad(script, DEF_SAMPLE_KEY));
        } else {
            return pool.jedis(jedis -> jedis.scriptLoad(script));
        }
    }

}
