#include "errdb.h"

#include <signal.h>

/*-----------------------------------------------------------------------------
 * C-level DB API
 *----------------------------------------------------------------------------*/

/* Important notes on lookup and disk store.
 *
 * When disk store is enabled on lookup we can have different cases.
 *
 * a) The key is in memory:
 *    - If the key is not in IO_SAVEINPROG state we can access it.
 *      As if it's just IO_SAVE this means we have the key in the IO queue
 *      but can't be accessed by the IO thread (it requires to be
 *      translated into an IO Job by the cache cron function.)
 *    - If the key is in IO_SAVEINPROG we can't touch the key and have
 *      to blocking wait completion of operations.
 * b) The key is not in memory:
 *    - If it's marked as non existing on disk as well (negative cache)
 *      we don't need to perform the disk access.
 *    - if the key MAY EXIST, but is not in memory, and it is marked as IO_SAVE
 *      then the key can only be a deleted one. As IO_SAVE keys are never
 *      evicted (dirty state), so the only possibility is that key was deleted.
 *    - if the key MAY EXIST we need to blocking load it.
 *      We check that the key is not in IO_SAVEINPROG state before accessing
 *      the disk object. If it is in this state, we wait.
 */

void lookupWaitBusyKey(redisDb *db, sds key) {
    /* FIXME: wait just for this key, not everything */
    //TODO: FIX Later
    //waitEmptyIOJobsQueue();
    //processAllPendingIOJobs();
    //redisAssert((cacheScheduleIOGetFlags(db,key) & REDIS_IO_SAVEINPROG) == 0);
}

tsObj *lookupKey(redisDb *db, sds key) {
    dictEntry *de = dictFind(db->dict, key);
    if (de) {
        tsObj *val = dictGetEntryVal(de);

        /* Update the access time for the aging algorithm.
         * Don't do it if we have a saving child, as this will trigger
         * a copy on write madness. */
        if (server.bgsavechildpid == -1 && server.bgrewritechildpid == -1)
            ;
            //val->lru = server.lruclock;

        //TODO: fix later
        //if (server.ds_enabled &&
        //    cacheScheduleIOGetFlags(db,key) & REDIS_IO_SAVEINPROG)
        //{
            /* Need to wait for the key to get unbusy */
        //    redisLog(REDIS_DEBUG,"Lookup found a key in SAVEINPROG state. Waiting. (Key was in the cache)");
        //    lookupWaitBusyKey(db,key);
        //}
        server.stat_keyspace_hits++;
        return val;
    } else {
        time_t expire;
        tsObj *val;

        /* Key not found in the in memory hash table, but if disk store is
         * enabled we may have this key on disk. If so load it in memory
         * in a blocking way. */
        //TODO: FIX LATER
        //if (server.ds_enabled && cacheKeyMayExist(db,key)) {
        //    long flags = cacheScheduleIOGetFlags(db,key);

            /* They key is not in cache, but it has a SAVE op in queue?
             * The only possibility is that the key was deleted, since
             * dirty keys are not evicted. */
        //    if (flags & REDIS_IO_SAVE) {
        //        server.stat_keyspace_misses++;
        //        return NULL;
        //    }

            /* At this point we need to blocking load the key in memory.
             * The first thing we do is waiting here if the key is busy. */
        //    if (flags & REDIS_IO_SAVEINPROG) {
        //        redisLog(REDIS_DEBUG,"Lookup found a key in SAVEINPROG state. Waiting (while force loading).");
        //        lookupWaitBusyKey(db,key);
        //    }

        //   redisLog(REDIS_DEBUG,"Force loading key %s via lookup", key);
        //    val = dsGet(db,key,&expire);
        //    if (val) {
        //        int retval = dbAdd(db,key,val);
        //        redisAssert(retval == REDIS_OK);
        //        if (expire != -1) setExpire(db,key,expire);
        //        server.stat_keyspace_hits++;
        //        return val;
        //    } else {
        //        cacheSetKeyDoesNotExist(db,key);
        //    }
        //}
        server.stat_keyspace_misses++;
        return NULL;
    }
}

tsObj *lookupKeyRead(redisDb *db, sds key) {
    expireIfNeeded(db,key);
    return lookupKey(db,key);
}

tsObj *lookupKeyWrite(redisDb *db, sds key) {
    expireIfNeeded(db,key);
    return lookupKey(db,key);
}

tsObj *lookupKeyReadOrReply(redisClient *c, sds key, tsObj *reply) {
    tsObj *o = lookupKeyRead(c->db, key);
    if (!o) addReplyObj(c,reply);
    return o;
}

tsObj *lookupKeyWriteOrReply(redisClient *c, sds key, tsObj *reply) {
    tsObj *o = lookupKeyWrite(c->db, key);
    if (!o) addReplyObj(c,reply);
    return o;
}

/* Add the key to the DB. If the key already exists REDIS_ERR is returned,
 * otherwise REDIS_OK is returned, and the caller should increment the
 * refcount of 'val'. */
int dbAdd(redisDb *db, sds key, tsObj *val) {
    /* Perform a lookup before adding the key, as we need to copy the
     * key value. */
    if (dictFind(db->dict, key) != NULL) {
        return REDIS_ERR;
    } else {
        sds copy = sdsdup(key);
        dictAdd(db->dict, copy, val);
        //TODO:FIX LATER
        //if (server.ds_enabled) cacheSetKeyMayExist(db,key);
        return REDIS_OK;
    }
}

/* If the key does not exist, this is just like dbAdd(). Otherwise
 * the value associated to the key is replaced with the new one.
 *
 * On update (key already existed) 0 is returned. Otherwise 1. */
int dbReplace(redisDb *db, sds key, tsObj *val) {
    tsObj *oldval;
    int retval;

    if ((oldval = dictFetchValue(db->dict,key)) == NULL) {
        sds copy = sdsdup(key);
        dictAdd(db->dict, copy, val);
        retval = 1;
    } else {
        dictReplace(db->dict, key, val);
        retval = 0;
    }
    //TODO: FIX LATER
    //if (server.ds_enabled) cacheSetKeyMayExist(db,key);
    return retval;
}

int dbExists(redisDb *db, sds key) {
    return dictFind(db->dict,key) != NULL;
}

/* Return a random key, in form of a Redis object.
 * If there are no keys, NULL is returned.
 *
 * The function makes sure to return keys not already expired. */
sds dbRandomKey(redisDb *db) {
    struct dictEntry *de;

    while(1) {
        sds key;

        de = dictGetRandomKey(db->dict);
        if (de == NULL) return NULL;

        key = dictGetEntryKey(de);
        if (dictFind(db->expires,key)) {
            expireIfNeeded(db,key);
        }
        return key;
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB */
int dbDelete(redisDb *db, sds key) {
    /* If diskstore is enabled make sure to awake waiting clients for this key
     * as it is not really useful to wait for a key already deleted to be
     * loaded from disk. */
    //TODO: fix later
    //if (server.ds_enabled) {
    //    handleClientsBlockedOnSwappedKey(db, key);
    //    cacheSetKeyDoesNotExist(db,key);
    //}
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    if (dictSize(db->expires) > 0) dictDelete(db->expires,key);
    return dictDelete(db->dict,key) == DICT_OK;
}

/* Empty the whole database.
 * If diskstore is enabled this function will just flush the in-memory cache. */
long long emptyDb() {
    long long removed = 0;

    removed += dictSize(server.db->dict);
    dictEmpty(server.db->dict);
    dictEmpty(server.db->expires);
    if (server.ds_enabled) dictEmpty(server.db->io_negcache);

    return removed;
}

/*-----------------------------------------------------------------------------
 * Hooks for key space changes.
 *
 * Every time a key in the database is modified the function
 * signalModifiedKey() is called.
 *
 * Every time a DB is flushed the function signalFlushDb() is called.
 *----------------------------------------------------------------------------*/

void signalModifiedKey(redisDb *db, sds key) {
    //TODO: FIX LATER
    //if (server.ds_enabled)
    //    cacheScheduleIO(db,key,REDIS_IO_SAVE);
}

void signalFlushedDb(int dbid) {
}

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *----------------------------------------------------------------------------*/

void flushdbCommand(redisClient *c) {
    server.dirty += dictSize(c->db->dict);
    signalFlushedDb(c->db->id);
    dictEmpty(c->db->dict);
    dictEmpty(c->db->expires);
    //TODO: fix later
    //if (server.ds_enabled) dsFlushDb(c->db->id);
    addReplySds(c,shared.ok);
}

void flushallCommand(redisClient *c) {
    signalFlushedDb(-1);
    server.dirty += emptyDb();
    addReplySds(c,shared.ok);
    if (server.bgsavechildpid != -1) {
        kill(server.bgsavechildpid,SIGKILL);
        //TODO: fix later
        //rdbRemoveTempFile(server.bgsavechildpid);
    }
    if (server.ds_enabled)
        ;
        //TODO: fix later
        //dsFlushDb(-1);
    server.dirty++;
}

void delCommand(redisClient *c) {
    int deleted = 0, j;

    for (j = 1; j < c->argc; j++) {
        if (server.ds_enabled) {
            lookupKeyRead(c->db,c->argv[j]);
            /* FIXME: this can be optimized a lot, no real need to load
             * a possibly huge value. */
        }
        if (dbDelete(c->db,c->argv[j])) {
            signalModifiedKey(c->db,c->argv[j]);
            server.dirty++;
            deleted++;
        } else if (server.ds_enabled) {
            //TODO: fix later
            //if (cacheKeyMayExist(c->db,c->argv[j]) &&
            //    dsExists(c->db,c->argv[j]))
            //{
            //    cacheScheduleIO(c->db,c->argv[j],REDIS_IO_SAVE);
            //    deleted = 1;
            //}
        }
    }
    addReplyLongLong(c,deleted);
}

void existsCommand(redisClient *c) {
    expireIfNeeded(c->db,c->argv[1]);
    if (dbExists(c->db,c->argv[1])) {
        addReplySds(c, shared.cone);
    } else {
        addReplySds(c, shared.czero);
    }
}

void randomkeyCommand(redisClient *c) {
    sds key;

    if ((key = dbRandomKey(c->db)) == NULL) {
        addReplySds(c,shared.nullbulk);
        return;
    }

    addReplySds(c,key);
}

void keysCommand(redisClient *c) {
    dictIterator *di;
    dictEntry *de;
    sds pattern = c->argv[1];
    int plen = sdslen(pattern), allkeys;
    unsigned long numkeys = 0;
    //TODO: FIX LATER 
    //void *replylen = addDeferredMultiBulkLength(c);

    //di = dictGetIterator(c->db->dict);
    //allkeys = (pattern[0] == '*' && pattern[1] == '\0');
    //while((de = dictNext(di)) != NULL) {
    //    sds key = dictGetEntryKey(de);

    //    if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
    //        if (expireIfNeeded(c->db,key) == 0) {
    //            addReplySds(c, key);
    //            numkeys++;
    //        }
    //    }
    //}
    //dictReleaseIterator(di);
    //TODO: FIX LATER
    //setDeferredMultiBulkLength(c,replylen,numkeys);
}

void dbsizeCommand(redisClient *c) {
    addReplyLongLong(c,dictSize(c->db->dict));
}

void lastsaveCommand(redisClient *c) {
    addReplyLongLong(c,server.lastsave);
}

void shutdownCommand(redisClient *c) {
    if (prepareForShutdown() == REDIS_OK)
        exit(0);
    addReplyError(c,"Errors trying to SHUTDOWN. Check logs.");
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

int removeExpire(redisDb *db, sds key) {
    /* An expire may only be removed if there is a corresponding entry in the
     * main dict. Otherwise, the key will never be freed. */
    redisAssert(dictFind(db->dict,key) != NULL);
    return dictDelete(db->expires,key) == DICT_OK;
}

void setExpire(redisDb *db, sds key, time_t when) {
    dictEntry *de;

    /* Reuse the sds from the main dict in the expire dict */
    de = dictFind(db->dict,key);
    redisAssert(de != NULL);
    dictReplace(db->expires,dictGetEntryKey(de),(void*)when);
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
time_t getExpire(redisDb *db, sds key) {
    dictEntry *de;

    /* No expire? return ASAP */
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key)) == NULL) return -1;

    /* The entry was found in the expire dict, this means it should also
     * be present in the main dict (safety check). */
    redisAssert(dictFind(db->dict,key) != NULL);
    return (time_t) dictGetEntryVal(de);
}

int expireIfNeeded(redisDb *db, sds key) {
    time_t when = getExpire(db,key);

    if (when < 0) return 0; /* No expire for this key */

    /* Return when this key has not expired */
    if (time(NULL) <= when) return 0;

    /* Delete the key */
    server.stat_expiredkeys++;
    return dbDelete(db,key);
}

/*-----------------------------------------------------------------------------
 * Expires Commands
 *----------------------------------------------------------------------------*/

void expireGenericCommand(redisClient *c, sds key, long seconds, long offset) {
    dictEntry *de;

    seconds -= offset;

    de = dictFind(c->db->dict,key);
    if (de == NULL) {
        addReplySds(c,shared.czero);
        return;
    }
    if (seconds <= 0) {
        if (dbDelete(c->db,key)) server.dirty++;
        addReplySds(c, shared.cone);
        signalModifiedKey(c->db,key);
        return;
    } else {
        time_t when = time(NULL)+seconds;
        setExpire(c->db,key,when);
        addReplySds(c,shared.cone);
        signalModifiedKey(c->db,key);
        server.dirty++;
        return;
    }
}

void expireCommand(redisClient *c) {
    expireGenericCommand(c,c->argv[1],atoi(c->argv[2]),0);
}

void expireatCommand(redisClient *c) {
    expireGenericCommand(c,c->argv[1],atoi(c->argv[2]),time(NULL));
}

void ttlCommand(redisClient *c) {
    time_t expire, ttl = -1;

    if (server.ds_enabled) lookupKeyRead(c->db,c->argv[1]);
    expire = getExpire(c->db,c->argv[1]);
    if (expire != -1) {
        ttl = (expire-time(NULL));
        if (ttl < 0) ttl = -1;
    }
    addReplyLongLong(c,(long long)ttl);
}

void persistCommand(redisClient *c) {
    dictEntry *de;

    de = dictFind(c->db->dict,c->argv[1]);
    if (de == NULL) {
        addReplySds(c,shared.czero);
    } else {
        if (removeExpire(c->db,c->argv[1])) {
            addReplySds(c,shared.cone);
            server.dirty++;
        } else {
            addReplySds(c,shared.czero);
        }
    }
}
