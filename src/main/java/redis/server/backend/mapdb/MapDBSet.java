package redis.server.backend.mapdb;

import org.mapdb.DB;
import org.mapdb.Fun;

import java.util.NavigableSet;

/**
 * Created by blafountain on 11/8/2014.
 */
public class MapDBSet {
    public static NavigableSet<byte[]> get(DB db, byte[] key, boolean create) {
        String keyStr = new String(key);

        // TODO: we should/could implement a 'small' set where if the number of values
        //  size is under a certian ammout we could just use a common entry (__sets), sort of what we do for
        //  __values. Only if we 'need' to have a root key like this should we use it. Redis does
        //  something similar with their ziplists
        if (create) {
            return db.createTreeSet(keyStr)
                    .comparator(Fun.BYTE_ARRAY_COMPARATOR)
                    .counterEnable()
                    .makeOrGet();
        } else {
            return db.getTreeSet(keyStr);
        }
    }
}
