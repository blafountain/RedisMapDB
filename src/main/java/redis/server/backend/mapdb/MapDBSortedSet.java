package redis.server.backend.mapdb;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Fun;

import java.io.Serializable;
import java.util.*;

/**
 * Created by blafountain on 11/7/2014.
 */
public class MapDBSortedSet {
    protected DB db;
    protected byte[] key;

    protected BTreeMap<byte[], Double> memberToValue;
    // TODO: we can use redis.server.backend.mapdb.bindings
    protected NavigableSet<Object[]> scoreToMembers;

    public MapDBSortedSet(DB db, byte[] key, BTreeMap<byte[], Double> memberToValue, NavigableSet<Object[]> scoreToMembers) {
        this.db = db;
        this.key = key;
        this.memberToValue = memberToValue;
        this.scoreToMembers = scoreToMembers;
    }

    public int removeMembers(byte[][] members) {
        int count = 0;
        for (byte[] member : members) {
            if (memberToValue.containsKey(member)) {
                Double score = memberToValue.get(member);

                memberToValue.remove(member);
                scoreToMembers.remove(new Object[]{score, member});
                count++;
            }
        }
        db.commit();
        return count;
    }

    public int removeEntries(SortedSet<Object[]> entries) {
        int count = 0;
        for (Object[] entry : entries) {
            memberToValue.remove((byte[]) entry[1]);
            scoreToMembers.remove(entry);
            count++;
        }
        db.commit();
        return count;
    }

    public double getScore(byte[] member) {
        return memberToValue.get(member);
    }

    public boolean isEmpty() {
        return memberToValue.isEmpty();
    }

    public Iterator<Object[]> getStart(int start, boolean descending) {
        // TODO: this is what we want...
        // scoreToMember.subMapIndex(start, end);
        // TODO: for now we're going to brute force it
        Iterator<Object[]> ret;

        if (descending) {
            ret = scoreToMembers.descendingIterator();
        } else {
            ret = scoreToMembers.iterator();
        }

        int cur = 0;
        while (cur < start) {
            ret.next();
            cur++;
        }
        return ret;
    }

    public NavigableSet<Object[]> subMap(double min, boolean minInclusive, double max, boolean maxExclusive) {
        return scoreToMembers.subSet(new Object[]{min, null}, minInclusive, new Object[]{max, null}, maxExclusive);
    }

    public SortedSet<Object[]> subMap(double min, double max) {
        return scoreToMembers.subSet(new Object[]{min, null}, new Object[]{max, null});
    }

    public SortedSet<Object[]> subMap(double min, double max, boolean descending) {
        if (descending) {
            return scoreToMembers.descendingSet().subSet(new Object[]{min, null}, new Object[]{max, null});
        } else {
            return scoreToMembers.subSet(new Object[]{min, null}, new Object[]{max, null});
        }
    }

    public boolean exists(byte[] member) {
        return memberToValue.containsKey(member);
    }

    public int getRank(byte[] member, boolean descending) {
        Double value = memberToValue.get(member);

        // TODO: fix me, this is not a real implementation
        int rank = 0;
        NavigableSet<Object[]> set;

        if (descending) {
            set = scoreToMembers.descendingSet();
        } else {
            set = scoreToMembers;
        }

        for (Object[] obj : set) {
            if (!descending && value <= (Double) obj[0]) {
                return rank;
            } else if (descending && value >= (Double) obj[0]) {
                return rank;
            }
            rank++;
        }
        return -1;
    }

    public boolean add(byte[] member, double value) {
        Double oldValue = memberToValue.get(member);

        memberToValue.put(member, value);
        if (oldValue != null) {
            scoreToMembers.remove(new Object[]{oldValue, member});
        }
        scoreToMembers.add(new Object[]{value, member});
        db.commit();
        return true;
    }

    public int size() {
        return memberToValue.size();
    }

    public static MapDBSortedSet get(DB db, byte[] key, boolean create) {
        String keyStr = new String(key);
        String memberStr = keyStr + ".member";
        String scoreStr = keyStr + ".score";

        BTreeMap<byte[], Double> memberToValue;
        NavigableSet<Object[]> scoreToMembers;

        if (create) {
            memberToValue = db.createTreeMap(memberStr)
                    .comparator(Fun.BYTE_ARRAY_COMPARATOR)
                    .counterEnable()
                    .makeOrGet();
            scoreToMembers = db.createTreeSet(scoreStr)
                    .comparator(new ScoreComparator())
                    .makeOrGet();
        } else {
            memberToValue = db.getTreeMap(memberStr);
            scoreToMembers = db.getTreeSet(scoreStr);
        }
        return new MapDBSortedSet(db, key, memberToValue, scoreToMembers);
    }

    public static class ScoreComparator implements Comparator<Object[]>, Serializable {
        public ScoreComparator() {
        }

        @Override
        public int compare(Object[] o1, Object[] o2) {
            if (o1 == o2) return 0;
            int valueCompare = Fun.COMPARATOR.compare(o1[0], o2[0]);

            if (valueCompare == 0) {
                if (o1[1] == o2[1]) {
                    return 0;
                } else if (o1[1] == null) {
                    return 1;
                } else if (o2[1] == null) {
                    return -1;
                }
                return Fun.BYTE_ARRAY_COMPARATOR.compare((byte[]) o1[1], (byte[]) o2[1]);
            }
            return valueCompare;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            return true;
        }
    }
}
