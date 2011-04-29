#include "errdb.h"

/*-----------------------------------------------------------------------------
 * List API
 *----------------------------------------------------------------------------*/
typedef struct tvNode {
    robj *t;
    robj *v;
} tvNode;

size_t robjLen(robj *obj) {
    size_t len;

    if (obj->encoding == REDIS_ENCODING_RAW) {
        len = sdslen(obj->ptr);
    } else {
        long n = (long)obj->ptr;

        /* Compute how many bytes will take this integer as a radix 10 string */
        len = 1;
        if (n < 0) {
            len++;
            n = -n;
        }
        while((n = n/10) != 0) {
            len++;
        }
    }

    return len;
}

size_t tvNodeLen(tvNode *tv) {
    return (size_t)(robjLen(tv->t) + robjLen(tv->v));
}

tvNode *createTvNode(robj *t, robj *v) {
    tvNode *n;
    if ((n = zmalloc(sizeof(*n))) == NULL)
        return NULL;
    n->t = t;
    n->v = v;
    incrRefCount(t);
    incrRefCount(v);
    return n;
}

tvNode *tsListTypeLast(robj *subject) {
    robj *value = NULL;
    list *list = subject->ptr;
    listNode *ln = listFirst(list);
    if (ln != NULL) {
        value = listNodeValue(ln);
    }
    return value;
}

void tsListInsert(robj *subject, robj *ts, robj *v) {
    tvNode *tv = createTvNode(ts, v);
    listAddNodeHead(subject->ptr, tv);
}

unsigned long listTypeLength(robj *subject) {
    if (subject->encoding == REDIS_ENCODING_LINKEDLIST) {
        return listLength((list*)subject->ptr);
    } else {
        redisPanic("Unknown list encoding");
    }
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
        decrRefCount(c->bpop.keys[j]);
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
listTypeIterator *listTypeInitIterator(robj *subject, int index, unsigned char direction) {
    listTypeIterator *li = zmalloc(sizeof(listTypeIterator));
    li->subject = subject;
    li->encoding = subject->encoding;
    li->direction = direction;
    if(li->encoding == REDIS_ENCODING_LINKEDLIST) {
        li->ln = listIndex(subject->ptr,index);
    } else {
        redisPanic("Unknown list encoding");
    }
    return li;
}

robj *listTypeGet(listTypeEntry *entry) {
    listTypeIterator *li = entry->li;
    robj *value = NULL;
    if (li->encoding == REDIS_ENCODING_LINKEDLIST) {
        redisAssert(entry->ln != NULL);
        value = listNodeValue(entry->ln);
        incrRefCount(value);
    } else {
        redisPanic("Unknown list encoding");
    }
    return value;
}

int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
    /* Protect from converting when iterating */
    redisAssert(li->subject->encoding == li->encoding);

    entry->li = li;
    if (li->encoding == REDIS_ENCODING_LINKEDLIST) {
        entry->ln = li->ln;
        if (entry->ln != NULL) {
            if (li->direction == REDIS_TAIL)
                li->ln = li->ln->next;
            else
                li->ln = li->ln->prev;
            return 1;
        }
    } else {
        redisPanic("Unknown list encoding");
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
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.czero);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;
    addReplyLongLong(c,listTypeLength(o));
}

void tsInsertCommand(redisClient *c) {
    robj *lobj = lookupKeyWrite(c->db,c->argv[1]);
    if (lobj == NULL) {
        lobj = createListObject();
        dbAdd(c->db,c->argv[1],lobj);
    } else {
        if (lobj->type != REDIS_LIST) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
    }
    tsListInsert(lobj, c->argv[2], c->argv[3]);
    addReplyLongLong(c, listTypeLength(lobj));
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

void tsFetchCommand(redisClient *c) {
    robj *o;
    int start = atoi(c->argv[2]->ptr);
    int end = atoi(c->argv[3]->ptr);
    int llen;
    int rangelen;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
         || checkType(c,o,REDIS_LIST)) return;
    llen = listTypeLength(o);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c,rangelen);
    if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        listNode *ln = listIndex(o->ptr,start);

        while(rangelen--) {
            tvNode *tv = ln->value;
            addReplyLen(c,tvNodeLen(tv)+1);
            addReply(c,tv->t);
            addReplyString(c, ":", 1);
            addReply(c,tv->v);
            addReply(c,shared.crlf);
            ln = ln->next;
        }
    } else {
        redisPanic("List encoding is not LINKEDLIST");
    }
}

void tsLastCommand(redisClient *c) {
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;

    tvNode *tv = tsListTypeLast(o);
    if (tv == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        addReplyLen(c,tvNodeLen(tv)+1);
        addReply(c,tv->t);
        addReplyString(c, ":", 1);
        addReply(c,tv->v);
        addReply(c,shared.crlf);
    }
}

