package redis.server.backend.mapdb;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;

/**
 * Created by blafountain on 11/14/2014.
 */
public class MapDBRedisBuilder {
    public static DB generateDB(String location, boolean memory, boolean transactions) {
        DBMaker db = null;

        if(memory) {
            db = DBMaker.newMemoryDB();
        } else {
            db = DBMaker.newFileDB(new File(location));
            db.mmapFileEnableIfSupported();
        }

        //if(!transactions)
        {
            db.transactionDisable();
        }

        db.closeOnJvmShutdown();
        return db.make();
    }
}
