package redis.server.backend.mapdb;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Fun;

/**
 * Created by blafountain on 11/7/2014.
 */
public class MapDBHash {
    public static BTreeMap<byte[], byte[]> get(DB db, byte[] key, boolean create) {
        String keyStr = new String(key);

        // TODO: we should/could implement a 'small' hashmap where if the number of key/value
        //  size is under a certian ammout we could just use a common map (__maps), sort of what we do for
        //  __values. Only if we 'need' to have a root key like this should we use it
        if (create) {
            return db.createTreeMap(keyStr)
                    .comparator(Fun.BYTE_ARRAY_COMPARATOR)
                    .counterEnable()
                    .makeOrGet();
        } else {
            return db.getTreeMap(keyStr);
        }
    }
}
