#include "redis.h"

/*-----------------------------------------------------------------------------
 * List API
 *----------------------------------------------------------------------------*/
robj *tsListTypeLast(robj *subject) {
    robj *value = NULL;
    list *list = subject->ptr;
    listNode *ln = listFirst(list);
    if (ln != NULL) {
        value = listNodeValue(ln);
    }
    return value;
}

void tsListInsert(robj *subject, robj *ts, robj *v) {
    listAddNodeHead(subject->ptr, v);
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/
void tsInsertCommand(redisClient *c) {
    robj *lobj = lookupKeyWrite(c->db,c->argv[1]);
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    if (lobj == NULL) {
        lobj = createListObject();
        dbAdd(c->db,c->argv[1],lobj);
    } else {
        if (lobj->type != REDIS_LIST) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
    }
    tsListInsert(lobj,c->argv[2],c->argv[3]);
    addReplyLongLong(c, listTypeLength(lobj));
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

void tsFetchCommand(redisClient *c) {
    //TODO:
    addReply(c,shared.nullbulk);
}

void tsLastCommand(redisClient *c) {
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;

    robj *value = tsListTypeLast(o);
    if (value == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        addReplyBulk(c,value);
    }
}

