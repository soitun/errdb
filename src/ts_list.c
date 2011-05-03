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
        dbAdd(c->db, c->argv[1], list);
    }
    tsListInsert(list, time, val);
    addReplyLongLong(c, listTypeLength(list));
    //TODO: fix later
    //signalModifiedKey(c->db, key);
    //server.dirty++;
}

void tsFetchCommand(redisClient *c) {
    list *list;
    int start = atoi(c->argv[2]);
    int end = atoi(c->argv[3]);
    int llen;
    int rangelen;

    if ((list = lookupKeyRead(c->db,c->argv[1])) == NULL) return;
    llen = listTypeLength(list);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        addReplySds(c,shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c,rangelen);

    listNode *ln = listIndex(list,start);

    while(rangelen--) {
        tsObj *o = ln->value;
        tsObjReply(c, o);
        ln = ln->next;
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

