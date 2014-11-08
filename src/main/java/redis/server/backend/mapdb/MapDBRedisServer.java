package redis.server.backend.mapdb;

import io.netty.buffer.ByteBuf;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import redis.server.RedisException;
import redis.server.RedisServer;
import redis.server.reply.*;

import java.io.File;
import java.util.*;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.MAX_VALUE;
import static redis.server.reply.IntegerReply.integer;
import static redis.server.reply.StatusReply.OK;
import static redis.util.Encoding.bytesToNum;
import static redis.util.Encoding.numToBytes;

/**
 * Created by blafountain on 11/6/2014.
 */
public class MapDBRedisServer implements RedisServer {
    protected DB db;

    // TODO: should keys contain metadata, not just a set?
    protected NavigableSet<byte[]> keys;
    protected Map<byte[], byte[]> values;

    public MapDBRedisServer() {
        db = DBMaker.newFileDB(new File("d:/redis.server.backend.mapdb/redis"))
                .mmapFileEnableIfSupported()
                .closeOnJvmShutdown()
                .make();

        // TODO: we can use redis.server.backend.mapdb.bindings
        keys = db.createTreeSet("__keys")
                .comparator(Fun.BYTE_ARRAY_COMPARATOR)
                .makeOrGet();
        values = db.createTreeMap("__values")
                .comparator(Fun.BYTE_ARRAY_COMPARATOR)
                .makeOrGet();
    }

    ////////////
    /// keys
    ////////////

    @Override
    public MultiBulkReply keys(byte[] pattern0) throws RedisException {
        if (pattern0 == null) {
            throw new RedisException("wrong number of arguments for KEYS");
        }
        List<Reply<ByteBuf>> replies = new ArrayList<Reply<ByteBuf>>();
        Iterator<byte[]> it = keys.iterator();
        while (it.hasNext()) {
            byte[] bytes = it.next();
            if (matches(bytes, pattern0, 0, 0)) {
                replies.add(new BulkReply(bytes));
            }
        }
        return new MultiBulkReply(replies.toArray(new Reply[replies.size()]));
    }

    /////////////
    /// values
    ////////////

    @Override
    public BulkReply get(byte[] key0) throws RedisException {
        Object o = values.get(key0);
        if (o instanceof byte[]) {
            return new BulkReply((byte[]) o);
        }
        if (o == null) {
            return BulkReply.NIL_REPLY;
        } else {
            throw invalidValue();
        }
    }

    @Override
    public IntegerReply del(byte[][] key0) throws RedisException {
        int total = 0;
        // TODO: support deleting other key types (zset...)
        // db.get("key")
        for (byte[] bytes : key0) {
            Object remove = values.remove(bytes);

            keys.remove(bytes);
            if (remove != null) {
                total++;
            }
        }
        db.commit();
        return integer(total);
    }

    @Override
    public StatusReply set(byte[] key0, byte[] value1) throws RedisException {
        keys.add(key0);
        values.put(key0, value1);
        db.commit();
        return StatusReply.OK;
    }

    @Override
    public IntegerReply incr(byte[] key0) throws RedisException {
        return _change(key0, 1);
    }

    @Override
    public IntegerReply incrby(byte[] key0, byte[] increment1) throws RedisException {
        return _change(key0, bytesToNum(increment1));
    }

    @Override
    public BulkReply incrbyfloat(byte[] key0, byte[] increment1) throws RedisException {
        return _change(key0, _todouble(increment1));
    }

    @Override
    public IntegerReply decr(byte[] key0) throws RedisException {
        return _change(key0, -1);
    }

    @Override
    public IntegerReply decrby(byte[] key0, byte[] decrement1) throws RedisException {
        return _change(key0, -bytesToNum(decrement1));
    }

    private IntegerReply _change(byte[] key0, long delta) throws RedisException {
        Object o = values.get(key0);
        IntegerReply ret = null;
        if (o == null) {
            values.put(key0, numToBytes(delta, false));
            ret = integer(delta);
        } else if (o instanceof byte[]) {
            try {
                long integer = bytesToNum((byte[]) o) + delta;
                values.put(key0, numToBytes(integer, false));
                ret = integer(integer);
            } catch (IllegalArgumentException e) {
                throw new RedisException(e.getMessage());
            }
        } else {
            throw notInteger();
        }

        keys.add(key0);
        db.commit();
        return ret;
    }

    private BulkReply _change(byte[] key0, double delta) throws RedisException {
        Object o = values.get(key0);
        BulkReply ret = null;
        if (o == null) {
            byte[] bytes = _tobytes(delta);
            values.put(key0, bytes);
            ret = new BulkReply(bytes);
        } else if (o instanceof byte[]) {
            try {
                double number = _todouble((byte[]) o) + delta;
                byte[] bytes = _tobytes(number);
                values.put(key0, bytes);
                ret = new BulkReply(bytes);
            } catch (IllegalArgumentException e) {
                throw new RedisException(e.getMessage());
            }
        } else {
            throw notInteger();
        }
        db.commit();
        return ret;
    }

    //////////////////
    //// sorted set
    //////////////////

    @Override
    public IntegerReply zadd(byte[][] args) throws RedisException {
        if (args.length < 3 || (args.length - 1) % 2 == 1) {
            throw new RedisException("wrong number of arguments for 'zadd' command");
        }
        byte[] key = args[0];
        MapDBSortedSet zset = MapDBSortedSet.get(db, key, true);

        keys.add(key);

        // TODO: support add all in a single transaction
        int total = 0;
        for (int i = 1; i < args.length; i += 2) {
            byte[] value = args[i + 1];
            byte[] score = args[i];
            if (zset.add(value, _todouble(score))) {
                total++;
            }
        }
        db.commit();
        return integer(total);
    }

    @Override
    public IntegerReply zcard(byte[] key0) throws RedisException {
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, true);
        return integer(zset.size());
    }

    @Override
    public BulkReply zscore(byte[] key0, byte[] member1) throws RedisException {
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, false);
        return new BulkReply(_tobytes(zset.getScore(member1)));
    }

    @Override
    public Reply zrank(byte[] key0, byte[] member1) throws RedisException {
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, false);

        return integer(zset.getRank(member1, false));
    }

    @Override
    public Reply zrevrank(byte[] key0, byte[] member1) throws RedisException {
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, false);

        return integer(zset.getRank(member1, true));
    }

    @Override
    public MultiBulkReply zrange(byte[] key0, byte[] start1, byte[] stop2, byte[] withscores3) throws RedisException {
        if (key0 == null || start1 == null || stop2 == null) {
            throw new RedisException("invalid number of argumenst for 'zrange' command");
        }
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, false);
        List<Reply<ByteBuf>> list = _zrange(start1, stop2, withscores3, zset, false);

        return new MultiBulkReply(list.toArray(new Reply[list.size()]));
    }

    @Override
    public MultiBulkReply zrevrange(byte[] key0, byte[] start1, byte[] stop2, byte[] withscores3) throws RedisException {
        if (key0 == null || start1 == null || stop2 == null) {
            throw new RedisException("invalid number of argumenst for 'zrevrange' command");
        }
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, false);
        List<Reply<ByteBuf>> list = _zrange(start1, stop2, withscores3, zset, true);

        return new MultiBulkReply(list.toArray(new Reply[list.size()]));
    }

    @Override
    public IntegerReply zcount(byte[] key0, byte[] min1, byte[] max2) throws RedisException {
        if (key0 == null || min1 == null || max2 == null) {
            throw new RedisException("wrong number of arguments for 'zcount' command");
        }
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, false);
        Score min = _toscorerange(min1);
        Score max = _toscorerange(max2);

        return integer(zset.subMap(_todouble(min1), min.inclusive, _todouble(max2), max.inclusive).size());
    }

    @Override
    public BulkReply zincrby(byte[] key0, byte[] increment1, byte[] member2) throws RedisException {
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, true);
        double increment = _todouble(increment1);
        if (zset.exists(member2)) {
            zset.add(member2, increment);
            return new BulkReply(increment1);
        } else {
            double newValue = zset.getScore(member2) + increment;

            zset.add(member2, newValue);
            return new BulkReply(_tobytes(newValue));
        }
    }

    @Override
    public MultiBulkReply zrangebyscore(byte[] key0, byte[] min1, byte[] max2, byte[][] withscores_offset_or_count4) throws RedisException {
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, true);
        if (zset.isEmpty()) return MultiBulkReply.EMPTY;
        List<Reply<ByteBuf>> list = _zrangebyscore(min1, max2, withscores_offset_or_count4, zset, false);
        return new MultiBulkReply(list.toArray(new Reply[list.size()]));
    }

    @Override
    public IntegerReply zrem(byte[] key0, byte[][] member1) throws RedisException {
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, true);
        if (zset.isEmpty()) return integer(0);

        return integer(zset.removeMembers(member1));
    }

    @Override
    public IntegerReply zremrangebyscore(byte[] key0, byte[] min1, byte[] max2) throws RedisException {
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, false);
        if (zset.isEmpty()) return integer(0);
        Score min = _toscorerange(min1);
        Score max = _toscorerange(max2);
        SortedSet<Object[]> entries = zset.subMap(min.value, max.value);

        return integer(zset.removeEntries(entries));
    }

    @Override
    public MultiBulkReply zrevrangebyscore(byte[] key0, byte[] max1, byte[] min2, byte[][] withscores_offset_or_count4) throws RedisException {
        MapDBSortedSet zset = MapDBSortedSet.get(db, key0, false);
        if (zset.isEmpty()) return MultiBulkReply.EMPTY;
        List<Reply<ByteBuf>> list = _zrangebyscore(min2, max1, withscores_offset_or_count4, zset, true);
        return new MultiBulkReply(list.toArray(new Reply[list.size()]));
    }

    ///
    @Override
    public IntegerReply zinterstore(byte[] destination0, byte[] numkeys1, byte[][] key2) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply zremrangebyrank(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply zunionstore(byte[] destination0, byte[] numkeys1, byte[][] key2) throws RedisException {
        return null;
    }

    private List<Reply<ByteBuf>> _zrange(byte[] start1, byte[] stop2, byte[] withscores3, MapDBSortedSet zset, boolean reverse) throws RedisException {
        boolean withscores = _checkcommand(withscores3, "withscores", true);
        int size = zset.size();
        int start = _torange(start1, size);
        int end = _torange(stop2, size);
        Iterator<Object[]> iterator = zset.getStart(start, reverse);
        List<Reply<ByteBuf>> list = new ArrayList<Reply<ByteBuf>>();
        for (int i = 0; i < size; i++) {
            if (iterator.hasNext()) {
                Object[] next = iterator.next();
                if (i >= start && i <= end) {
                    list.add(new BulkReply((byte[]) next[1]));
                    if (withscores) {
                        list.add(new BulkReply(_tobytes((Double) next[0])));
                    }
                } else if (i > end) {
                    break;
                }
            }
        }
        return list;
    }

    private List<Reply<ByteBuf>> _zrangebyscore(byte[] min1, byte[] max2, byte[][] withscores_offset_or_count4, MapDBSortedSet zset, boolean reverse) throws RedisException {
        int position = 0;
        boolean withscores = false;
        if (withscores_offset_or_count4.length > 0) {
            withscores = _checkcommand(withscores_offset_or_count4[0], "withscores", false);
        }
        if (withscores) position++;
        boolean limit = false;
        if (withscores_offset_or_count4.length > position) {
            limit = _checkcommand(withscores_offset_or_count4[position++], "limit", true);
        }
        if (withscores_offset_or_count4.length != position + (limit ? 2 : 0)) {
            throw new RedisException("syntax error");
        }
        int offset = 0;
        int number = Integer.MAX_VALUE;
        if (limit) {
            offset = _toint(withscores_offset_or_count4[position++]);
            number = _toint(withscores_offset_or_count4[position]);
            if (offset < 0 || number < 1) {
                throw notInteger();
            }
        }
        Score min = _toscorerange(min1);
        Score max = _toscorerange(max2);
        SortedSet<Object[]> entries;

        if (reverse) {
            entries = zset.subMap(min.value, max.value, true);
        } else {
            entries = zset.subMap(min.value, max.value);
        }

        int current = 0;
        List<Reply<ByteBuf>> list = new ArrayList<Reply<ByteBuf>>();
        for (Object[] entry : entries) {
            if (current >= offset && current < offset + number) {
                list.add(new BulkReply((byte[]) entry[1]));
                if (withscores) list.add(new BulkReply(_tobytes((Double) entry[0])));
            }
            current++;
        }
        return list;
    }

    ////////////////
    //// hash
    ////////////////

    @Override
    public IntegerReply hset(byte[] key0, byte[] field1, byte[] value2) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, true);
        Object put = hash.put(field1, value2);

        db.commit();
        return put == null ? integer(1) : integer(0);
    }

    @Override
    public MultiBulkReply hgetall(byte[] key0) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, false);
        int size = hash.size();
        Reply[] replies = new Reply[size * 2];
        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : hash.entrySet()) {
            replies[i++] = new BulkReply(entry.getKey());
            replies[i++] = new BulkReply(entry.getValue());
        }
        return new MultiBulkReply(replies);
    }

    @Override
    public IntegerReply hdel(byte[] key0, byte[][] field1) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, false);
        int total = 0;
        for (byte[] hkey : field1) {
            total += hash.remove(hkey) == null ? 0 : 1;
        }
        db.commit();
        return integer(total);
    }

    @Override
    public IntegerReply hexists(byte[] key0, byte[] field1) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, false);
        return hash.get(field1) == null ? integer(0) : integer(1);
    }

    @Override
    public BulkReply hget(byte[] key0, byte[] field1) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, false);
        byte[] bytes = hash.get(field1);
        if (bytes == null) {
            return BulkReply.NIL_REPLY;
        } else {
            return new BulkReply(bytes);
        }
    }

    @Override
    public IntegerReply hincrby(byte[] key0, byte[] field1, byte[] increment2) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, true);
        byte[] field = hash.get(field1);
        int increment = _toint(increment2);
        int ret = increment;

        if (field == null) {
            hash.put(field1, increment2);
        } else {
            int i = _toint(field);
            int value = i + increment;

            ret = value;
            hash.put(field1, numToBytes(value, false));
        }
        db.commit();
        return new IntegerReply(ret);
    }

    @Override
    public BulkReply hincrbyfloat(byte[] key0, byte[] field1, byte[] increment2) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, true);
        byte[] field = hash.get(field1);
        double increment = _todouble(increment2);
        byte[] ret = increment2;

        if (field == null) {
            hash.put(field1, increment2);
        } else {
            double d = _todouble(field);
            double value = d + increment;
            byte[] bytes = _tobytes(value);

            ret = bytes;
            hash.put(field1, bytes);
        }
        db.commit();
        return new BulkReply(ret);
    }

    @Override
    public MultiBulkReply hkeys(byte[] key0) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, false);
        int size = hash.size();
        Reply[] replies = new Reply[size];
        int i = 0;
        for (byte[] hkey : hash.keySet()) {
            replies[i++] = new BulkReply(hkey);
        }
        return new MultiBulkReply(replies);
    }

    @Override
    public IntegerReply hlen(byte[] key0) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, false);
        return integer(hash.size());
    }

    @Override
    public MultiBulkReply hmget(byte[] key0, byte[][] field1) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, false);
        int length = field1.length;
        Reply[] replies = new Reply[length];
        for (int i = 0; i < length; i++) {
            byte[] bytes = hash.get(field1[i]);
            if (bytes == null) {
                replies[i] = BulkReply.NIL_REPLY;
            } else {
                replies[i] = new BulkReply(bytes);
            }
        }
        return new MultiBulkReply(replies);
    }

    @Override
    public StatusReply hmset(byte[] key0, byte[][] field_or_value1) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, true);
        if (field_or_value1.length % 2 != 0) {
            throw new RedisException("wrong number of arguments for HMSET");
        }
        for (int i = 0; i < field_or_value1.length; i += 2) {
            hash.put(field_or_value1[i], field_or_value1[i + 1]);
        }
        db.commit();
        return OK;
    }

    @Override
    public IntegerReply hsetnx(byte[] key0, byte[] field1, byte[] value2) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, true);
        byte[] bytes = hash.get(field1);
        if (bytes == null) {
            hash.put(field1, value2);
            db.commit();
            return integer(1);
        } else {
            return integer(0);
        }
    }

    @Override
    public MultiBulkReply hvals(byte[] key0) throws RedisException {
        Map<byte[], byte[]> hash = MapDBHash.get(db, key0, true);
        int size = hash.size();
        Reply[] replies = new Reply[size];
        int i = 0;
        for (byte[] hvalue : hash.values()) {
            replies[i++] = new BulkReply(hvalue);
        }
        return new MultiBulkReply(replies);
    }

    ////////////
    // set
    ////////////

    @Override
    public IntegerReply sadd(byte[] key0, byte[][] member1) throws RedisException {
        NavigableSet<byte[]> set = MapDBSet.get(db, key0, true);
        int total = 0;
        for (byte[] bytes : member1) {
            if (set.add(bytes)) total++;
        }
        db.commit();
        return integer(total);
    }

    @Override
    public IntegerReply scard(byte[] key0) throws RedisException {
        NavigableSet<byte[]> set = MapDBSet.get(db, key0, false);
        return integer(set.size());
    }

    @Override
    public IntegerReply sismember(byte[] key0, byte[] member1) throws RedisException {
        NavigableSet<byte[]> set = MapDBSet.get(db, key0, false);
        return set.contains(member1) ? integer(1) : integer(0);
    }

    @Override
    public MultiBulkReply smembers(byte[] key0) throws RedisException {
        NavigableSet<byte[]> set = MapDBSet.get(db, key0, false);
        return _setreply(set);
    }

    @Override
    public IntegerReply srem(byte[] key0, byte[][] member1) throws RedisException {
        NavigableSet<byte[]> set = MapDBSet.get(db, key0, false);
        int total = 0;
        for (byte[] member : member1) {
            if (set.remove(member)) {
                total++;
            }
        }
        return new IntegerReply(total);
    }

    @Override
    public MultiBulkReply sdiff(byte[][] key0) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply sdiffstore(byte[] destination0, byte[][] key1) throws RedisException {
        return null;
    }

    @Override
    public MultiBulkReply sinter(byte[][] key0) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply sinterstore(byte[] destination0, byte[][] key1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply smove(byte[] source0, byte[] destination1, byte[] member2) throws RedisException {
        return null;
    }

    @Override
    public BulkReply spop(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public Reply srandmember(byte[] key0, byte[] count1) throws RedisException {
        return null;
    }

    @Override
    public MultiBulkReply sunion(byte[][] key0) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply sunionstore(byte[] destination0, byte[][] key1) throws RedisException {
        return null;
    }

    private MultiBulkReply _setreply(NavigableSet<byte[]> set) {
        Reply[] replies = new Reply[set.size()];
        int i = 0;
        for (byte[] value : set) {
            replies[i++] = new BulkReply(value);
        }
        return new MultiBulkReply(replies);
    }


    ////////////
    // list
    ////////////

    @Override
    public IntegerReply lpush(byte[] key0, byte[][] value1) throws RedisException {
        return null;
    }

    @Override
    public MultiBulkReply blpop(byte[][] key0) throws RedisException {
        return null;
    }

    @Override
    public MultiBulkReply brpop(byte[][] key0) throws RedisException {
        return null;
    }

    @Override
    public BulkReply brpoplpush(byte[] source0, byte[] destination1, byte[] timeout2) throws RedisException {
        return null;
    }

    @Override
    public BulkReply lindex(byte[] key0, byte[] index1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply linsert(byte[] key0, byte[] where1, byte[] pivot2, byte[] value3) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply llen(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public BulkReply lpop(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply lpushx(byte[] key0, byte[] value1) throws RedisException {
        return null;
    }

    @Override
    public MultiBulkReply lrange(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply lrem(byte[] key0, byte[] count1, byte[] value2) throws RedisException {
        return null;
    }

    @Override
    public StatusReply lset(byte[] key0, byte[] index1, byte[] value2) throws RedisException {
        return null;
    }

    @Override
    public StatusReply ltrim(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {
        return null;
    }

    @Override
    public BulkReply rpop(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public BulkReply rpoplpush(byte[] source0, byte[] destination1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply rpush(byte[] key0, byte[][] value1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply rpushx(byte[] key0, byte[] value1) throws RedisException {
        return null;
    }

    /////////////
    /// util
    /////////////

    static class Score {
        boolean inclusive = true;
        double value;
    }

    private Score _toscorerange(byte[] specifier) {
        Score score = new Score();
        String s = new String(specifier).toLowerCase();
        if (s.startsWith("(")) {
            score.inclusive = false;
            s = s.substring(1);
        }
        if (s.equals("-inf")) {
            score.value = Double.NEGATIVE_INFINITY;
        } else if (s.equals("inf") || s.equals("+inf")) {
            score.value = Double.POSITIVE_INFINITY;
        } else {
            score.value = Double.parseDouble(s);
        }
        return score;
    }

    private static int _toint(byte[] offset1) throws RedisException {
        long offset = bytesToNum(offset1);
        if (offset > MAX_VALUE) {
            throw notInteger();
        }
        return (int) offset;
    }

    private static int _torange(byte[] offset1, int length) throws RedisException {
        long offset = bytesToNum(offset1);
        if (offset > MAX_VALUE) {
            throw notInteger();
        }
        if (offset < 0) {
            offset = (length + offset);
        }
        if (offset >= length) {
            offset = length - 1;
        }
        return (int) offset;
    }

    private boolean _checkcommand(byte[] check, String command, boolean syntax) throws RedisException {
        boolean result;
        if (check != null) {
            if (new String(check).toLowerCase().equals(command)) {
                result = true;
            } else {
                if (syntax) {
                    throw new RedisException("syntax error");
                } else {
                    return false;
                }
            }
        } else {
            result = false;
        }
        return result;
    }

    private byte[] _tobytes(double score) {
        return String.valueOf(score).getBytes();
    }

    private double _todouble(byte[] score) {
        return parseDouble(new String(score));
    }

    private static RedisException notInteger() {
        return new RedisException("value is not an integer or out of range");
    }

    private static RedisException invalidValue() {
        return new RedisException("Operation against a key holding the wrong kind of value");
    }

    private static boolean matches(byte[] key, byte[] pattern, int kp, int pp) {
        if (kp == key.length) {
            return pp == pattern.length || (pp == pattern.length - 1 && pattern[pp] == '*');
        } else if (pp == pattern.length) {
            return false;
        }
        byte c = key[kp];
        byte p = pattern[pp];
        switch (p) {
            case '?':
                // Always matches, move to next character in key and pattern
                return matches(key, pattern, kp + 1, pp + 1);
            case '*':
                // Matches this character than either matches end or try next character
                return matches(key, pattern, kp + 1, pp + 1) || matches(key, pattern, kp + 1, pp);
            case '\\':
                // Matches the escaped character and the rest
                return c == pattern[pp + 1] && matches(key, pattern, kp + 1, pp + 2);
            case '[':
                // Matches one of the characters and the rest
                boolean found = false;
                pp++;
                do {
                    byte b = pattern[pp++];
                    if (b == ']') {
                        break;
                    } else {
                        if (b == c) found = true;
                    }
                } while (true);
                return found && matches(key, pattern, kp + 1, pp);
            default:
                // This matches and the rest
                return c == p && matches(key, pattern, kp + 1, pp + 1);
        }
    }

    ///////////////
    ///////// all the rest... not implemented
    ///////////////

    //
    @Override
    public IntegerReply append(byte[] key0, byte[] value1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply bitcount(byte[] key0, byte[] start1, byte[] end2) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply bitop(byte[] operation0, byte[] destkey1, byte[][] key2) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply getbit(byte[] key0, byte[] offset1) throws RedisException {
        return null;
    }

    @Override
    public BulkReply getrange(byte[] key0, byte[] start1, byte[] end2) throws RedisException {
        return null;
    }

    @Override
    public BulkReply getset(byte[] key0, byte[] value1) throws RedisException {
        return null;
    }

    @Override
    public MultiBulkReply mget(byte[][] key0) throws RedisException {
        return null;
    }

    @Override
    public StatusReply mset(byte[][] key_or_value0) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply msetnx(byte[][] key_or_value0) throws RedisException {
        return null;
    }

    @Override
    public Reply psetex(byte[] key0, byte[] milliseconds1, byte[] value2) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply setbit(byte[] key0, byte[] offset1, byte[] value2) throws RedisException {
        return null;
    }

    @Override
    public StatusReply setex(byte[] key0, byte[] seconds1, byte[] value2) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply setnx(byte[] key0, byte[] value1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply setrange(byte[] key0, byte[] offset1, byte[] value2) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply strlen(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public BulkReply echo(byte[] message0) throws RedisException {
        return null;
    }

    @Override
    public StatusReply ping() throws RedisException {
        return null;
    }

    @Override
    public StatusReply quit() throws RedisException {
        return null;
    }

    @Override
    public StatusReply select(byte[] index0) throws RedisException {
        return null;
    }

    @Override
    public StatusReply bgrewriteaof() throws RedisException {
        return null;
    }

    @Override
    public StatusReply bgsave() throws RedisException {
        return null;
    }

    @Override
    public Reply client_kill(byte[] ip_port0) throws RedisException {
        return null;
    }

    @Override
    public Reply client_list() throws RedisException {
        return null;
    }

    @Override
    public Reply client_getname() throws RedisException {
        return null;
    }

    @Override
    public Reply client_setname(byte[] connection_name0) throws RedisException {
        return null;
    }

    @Override
    public Reply config_get(byte[] parameter0) throws RedisException {
        return null;
    }

    @Override
    public Reply config_set(byte[] parameter0, byte[] value1) throws RedisException {
        return null;
    }

    @Override
    public Reply config_resetstat() throws RedisException {
        return null;
    }

    @Override
    public IntegerReply dbsize() throws RedisException {
        return null;
    }

    @Override
    public Reply debug_object(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public Reply debug_segfault() throws RedisException {
        return null;
    }

    @Override
    public StatusReply flushall() throws RedisException {
        return null;
    }

    @Override
    public StatusReply flushdb() throws RedisException {
        return null;
    }

    @Override
    public BulkReply info(byte[] section0) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply lastsave() throws RedisException {
        return null;
    }

    @Override
    public Reply monitor() throws RedisException {
        return null;
    }

    @Override
    public Reply save() throws RedisException {
        return null;
    }

    @Override
    public StatusReply shutdown(byte[] NOSAVE0, byte[] SAVE1) throws RedisException {
        return null;
    }

    @Override
    public StatusReply slaveof(byte[] host0, byte[] port1) throws RedisException {
        return null;
    }

    @Override
    public Reply slowlog(byte[] subcommand0, byte[] argument1) throws RedisException {
        return null;
    }

    @Override
    public Reply sync() throws RedisException {
        return null;
    }

    @Override
    public MultiBulkReply time() throws RedisException {
        return null;
    }

    @Override
    public BulkReply dump(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply exists(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply expire(byte[] key0, byte[] seconds1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply expireat(byte[] key0, byte[] timestamp1) throws RedisException {
        return null;
    }

    @Override
    public StatusReply migrate(byte[] host0, byte[] port1, byte[] key2, byte[] destination_db3, byte[] timeout4) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply move(byte[] key0, byte[] db1) throws RedisException {
        return null;
    }

    @Override
    public Reply object(byte[] subcommand0, byte[][] arguments1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply persist(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply pexpire(byte[] key0, byte[] milliseconds1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply pexpireat(byte[] key0, byte[] milliseconds_timestamp1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply pttl(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public BulkReply randomkey() throws RedisException {
        return null;
    }

    @Override
    public StatusReply rename(byte[] key0, byte[] newkey1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply renamenx(byte[] key0, byte[] newkey1) throws RedisException {
        return null;
    }

    @Override
    public StatusReply restore(byte[] key0, byte[] ttl1, byte[] serialized_value2) throws RedisException {
        return null;
    }

    @Override
    public Reply sort(byte[] key0, byte[][] pattern1) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply ttl(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public StatusReply type(byte[] key0) throws RedisException {
        return null;
    }

    @Override
    public StatusReply unwatch() throws RedisException {
        return null;
    }

    @Override
    public StatusReply watch(byte[][] key0) throws RedisException {
        return null;
    }

    @Override
    public Reply eval(byte[] script0, byte[] numkeys1, byte[][] key2) throws RedisException {
        return null;
    }

    @Override
    public Reply evalsha(byte[] sha10, byte[] numkeys1, byte[][] key2) throws RedisException {
        return null;
    }

    @Override
    public Reply script_exists(byte[][] script0) throws RedisException {
        return null;
    }

    @Override
    public Reply script_flush() throws RedisException {
        return null;
    }

    @Override
    public Reply script_kill() throws RedisException {
        return null;
    }

    @Override
    public Reply script_load(byte[] script0) throws RedisException {
        return null;
    }

    @Override
    public IntegerReply publish(byte[] channel0, byte[] message1) throws RedisException {
        return null;
    }
}
