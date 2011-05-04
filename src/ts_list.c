#include "adlist.h"
#include "errdb.h"

/*-----------------------------------------------------------------------------
 * List API
 *----------------------------------------------------------------------------*/
size_t tsObjLen(tsObj *obj) {
    size_t len = 1;
    long n = (long)obj->time;
    printf("obj->time: %d\n", obj->time);
    printf("obj->value: %s\n", obj->value);
    /* Compute how many bytes will take this integer as a radix 10 string */
    while((n = n/10) != 0) {
        len++;
    }
    return len + sdslen(obj->value);
}

void tsObjReply(redisClient *c, tsObj *o) {
    sds ts = sdsfromlonglong(o->time);
    addReplyLen(c, sdslen(ts) + sdslen(o->value) + 1);
    addReplySds(c, ts);
    addReplyString(c, ":", 1);
    addReplySds(c, o->value);
    addReplySds(c,shared.crlf);
}

tsObj *createTsObject(int time, sds value) {
    printf("time: %d, value: %s \n", time, value);
    tsObj *o;
    if ((o = zmalloc(sizeof(*o))) == NULL)
        return NULL;
    o -> tag = 0;
    o->time = time;
    o->value  = sdsdup(value);
    return o;
}

tsObj *tsListLast(list *list) {
    tsObj *o = NULL;
    listNode *ln = listFirst(list);
    if (ln != NULL) {
        o = listNodeValue(ln);
    }
    return o;
}

void tsListInsert(list *list, int time, sds value) {
    tsObj *o;
    o = createTsObject(time, value);
    listAddNodeHead(list, o);
    list->lastTs = time;
}

unsigned long listTypeLength(list *list) {
    return listLength(list);
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP */
void unblockClientWaitingData(redisClient *c) {
    dictEntry *de;
    list *l;
    int j;

    redisAssert(c->bpop.keys != NULL);
    /* The client may wait for multiple keys, so unblock it for every key. */
    for (j = 0; j < c->bpop.count; j++) {
        /* Remove this client from the list of clients waiting for this key. */
        de = dictFind(c->db->blocking_keys,c->bpop.keys[j]);
        redisAssert(de != NULL);
        l = dictGetEntryVal(de);
        listDelNode(l,listSearchKey(l,c));
        /* If the list is empty we need to remove it to avoid wasting memory */
        if (listLength(l) == 0)
            dictDelete(c->db->blocking_keys,c->bpop.keys[j]);
    }

    /* Cleanup the client structure */
    zfree(c->bpop.keys);
    c->bpop.keys = NULL;
    c->bpop.target = NULL;
    c->flags &= ~REDIS_BLOCKED;
    c->flags |= REDIS_UNBLOCKED;
    server.bpop_blocked_clients--;
    listAddNodeTail(server.unblocked_clients,c);
}

/* Initialize an iterator at the specified index. */
listTypeIterator *listTypeInitIterator(list *subject, int index, unsigned char direction) {
    listTypeIterator *li = zmalloc(sizeof(listTypeIterator));
    li->subject = subject;
    li->direction = direction;
    li->ln = listIndex(subject,index);
    return li;
}

tsObj *listTypeGet(listTypeEntry *entry) {
    listTypeIterator *li = entry->li;
    tsObj *value = NULL;
    redisAssert(entry->ln != NULL);
    value = listNodeValue(entry->ln);
    return value;
}

int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
    entry->li = li;
    entry->ln = li->ln;
    if (entry->ln != NULL) {
        if (li->direction == REDIS_TAIL)
            li->ln = li->ln->next;
        else
            li->ln = li->ln->prev;
        return 1;
    }
    return 0;
}

/* Clean up the iterator. */
void listTypeReleaseIterator(listTypeIterator *li) {
    zfree(li);
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/
void llenCommand(redisClient *c) {
    tsObj *o = lookupKeyRead(c->db,c->argv[1]);
    if (o == NULL) return;
    addReplyLongLong(c,listTypeLength(o));
}

void tsInsertCommand(redisClient *c) {
    int time = atoi(c->argv[2]);
    sds val = c->argv[3];
    list *list = lookupKeyWrite(c->db, c->argv[1]);
    if (list == NULL) {
        list = listCreate();
        list->firstTs = time;
        dbAdd(c->db, c->argv[1], list);
    }
    tsListInsert(list, time, val);
    addReplyLongLong(c, listTypeLength(list));
    //TODO: fix later
    //signalModifiedKey(c->db, key);
    //server.dirty++;
}

void tsFetchCommand(redisClient *c) {
    int i = 0;
    list *list;
    listIter *iter;
    listNode *ln;
    int direct;
    tsObj *o;
    int start = atoi(c->argv[2]);
    int end = atoi(c->argv[3]);

    int start_offset;
    int end_offset;

    //TODO: FIX LATER
    tsObj *result[300];
    int found = 0;
    int items = 0;
    
    if ((list = lookupKeyRead(c->db,c->argv[1])) == NULL) {
        addReplySds(c,shared.emptymultibulk);
        return;
    }

    /* convert negative indexes */
    if (start < 0 || end < 0) {
        addReplySds(c,shared.emptymultibulk);
        return;
    }

    printf("start: %d, end: %d, lastTs: %d, firstTs: %d\n", start, end, list->lastTs, list->firstTs);

    if(start > end || start > list->lastTs || end < list->firstTs) {
        addReplySds(c,shared.emptymultibulk);
        return;
    }

    start_offset = start - list->firstTs;
    if(start_offset < 0) {
        start_offset = -start_offset;
    }
    end_offset = end - list->lastTs;
    if(end_offset < 0) {
        end_offset = -end_offset;
    }

    printf("start_offset: %d, end_offset: %d\n", start_offset, end_offset);

    direct = (start_offset < end_offset) ? AL_START_TAIL : AL_START_HEAD;
    iter = listGetIterator(list, direct);

    while((ln = listNext(iter)) != NULL) {
        if(items > 100) break;
        o = (tsObj *)ln->value;
        if(o->time <= end && o->time >= start) {
            found = 1;
            result[items++] = o;
        } else {
            if(found) break;
        }
    }

    printf("return items: %d", items);

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c, items);

    if(direct == AL_START_TAIL) {
      for(i = 0; i < items; i++) {
          tsObj *o = result[i];
          tsObjReply(c, o);
      }
    } else {
        for(i = items-1; i >= 0; i--) {
            tsObj *o = result[i];
            tsObjReply(c, o);
        }
    }

}

void tsLastCommand(redisClient *c) {
    sds key = c->argv[1];
    list *list = lookupKeyRead(c->db, key);
    if (list == NULL) {
        addReplySds(c, shared.nullbulk);
        return;
    }
    tsObj *o = tsListLast(list);
    printf("last return, o->time: %d, o->value: %s\n", o->time, o->value);
    tsObjReply(c, o);
}

