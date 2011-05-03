#include "errdb.h"
#include <sys/uio.h>

redisClient *createClient(int fd) {
    redisClient *c = zmalloc(sizeof(redisClient));
    c->bufpos = 0;

    anetNonBlock(NULL,fd);
    anetTcpNoDelay(NULL,fd);
    if (!c) return NULL;
    if (aeCreateFileEvent(server.el,fd,AE_READABLE,
        readQueryFromClient, c) == AE_ERR)
    {
        close(fd);
        zfree(c);
        return NULL;
    }

    c->fd = fd;
    c->db = server.db;
    c->querybuf = sdsempty();
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->lastinteraction = time(NULL);
    c->authenticated = 0;
    c->replstate = REDIS_REPL_NONE;
    c->reply = listCreate();
    listSetFreeMethod(c->reply,sdsfree);
    listSetDupMethod(c->reply,sdsdup);
    c->bpop.keys = NULL;
    c->bpop.count = 0;
    c->bpop.timeout = 0;
    c->bpop.target = NULL;
    c->io_keys = listCreate();
    listSetFreeMethod(c->io_keys,sdsfree);
    listAddNodeTail(server.clients, c);
    return c;
}

/* Set the event loop to listen for write events on the client's socket.
 * Typically gets called every time a reply is built. */
int _installWriteEvent(redisClient *c) {
    /* When CLOSE_AFTER_REPLY is set, no more replies may be added! */
    redisAssert(!(c->flags & REDIS_CLOSE_AFTER_REPLY));

    if (c->fd <= 0) return REDIS_ERR;
    if (c->bufpos == 0 && listLength(c->reply) == 0 &&
        (c->replstate == REDIS_REPL_NONE ||
         c->replstate == REDIS_REPL_ONLINE) &&
        aeCreateFileEvent(server.el, c->fd, AE_WRITABLE,
        sendReplyToClient, c) == AE_ERR) return REDIS_ERR;
    return REDIS_OK;
}

int _addReplyToBuffer(redisClient *c, char *s, size_t len) {
    size_t available = sizeof(c->buf)-c->bufpos;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    if (listLength(c->reply) > 0) return REDIS_ERR;

    /* Check that the buffer has enough space available for this string. */
    if (len > available) return REDIS_ERR;

    memcpy(c->buf+c->bufpos,s,len);
    c->bufpos+=len;
    return REDIS_OK;
}

void _addReplySdsToList(redisClient *c, sds s) {
    listAddNodeTail(c->reply, s);
}

void _addReplyStringToList(redisClient *c, char *s, size_t len) {
    listAddNodeTail(c->reply, sdsnewlen(s,len));
}

void addReplySds(redisClient *c, sds s) {
    if (_installWriteEvent(c) != REDIS_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    if (_addReplyToBuffer(c,s,sdslen(s)) == REDIS_OK) {
        sdsfree(s);
    } else {
        /* This method free's the sds when it is no longer needed. */
        _addReplySdsToList(c,s);
    }
}

void addReplyString(redisClient *c, char *s, size_t len) {
    if (_installWriteEvent(c) != REDIS_OK) return;
    if (_addReplyToBuffer(c,s,len) != REDIS_OK)
        _addReplyStringToList(c,s,len);
}

void _addReplyError(redisClient *c, char *s, size_t len) {
    addReplyString(c,"-ERR ",5);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyError(redisClient *c, char *err) {
    _addReplyError(c,err,strlen(err));
}

void addReplyErrorFormat(redisClient *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    _addReplyError(c,s,sdslen(s));
    sdsfree(s);
}

void _addReplyStatus(redisClient *c, char *s, size_t len) {
    addReplyString(c,"+",1);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyStatus(redisClient *c, char *status) {
    _addReplyStatus(c,status,strlen(status));
}

void addReplyStatusFormat(redisClient *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    _addReplyStatus(c,s,sdslen(s));
    sdsfree(s);
}

/* Add a duble as a bulk reply */
void addReplyDouble(redisClient *c, double d) {
    char dbuf[128], sbuf[128];
    int dlen, slen;
    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
    addReplyString(c,sbuf,slen);
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
void _addReplyLongLong(redisClient *c, long long ll, char prefix) {
    char buf[128];
    int len;
    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyString(c,buf,len+3);
}

void addReplyLen(redisClient *c, size_t l) {
    _addReplyLongLong(c,l,'$');
}

void addReplyLongLong(redisClient *c, long long ll) {
    _addReplyLongLong(c,ll,':');
}

void addReplyMultiBulkLen(redisClient *c, long length) {
    _addReplyLongLong(c,length,'*');
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(redisClient *c, sds sds) {
    _addReplyLongLong(c,sdslen(sds),'$');
}

/* Add a Redis Object as a bulk reply */
void addReplyBulk(redisClient *c, sds sds) {
    addReplyBulkLen(c,sds);
    addReplySds(c,sds);
    addReplySds(c,shared.crlf);
}

void addReplyObj(redisClient *c, tsObj *obj) {
    //TODO: FIX LATER
}

/* Add a C buffer as bulk reply */
void addReplyBulkCBuffer(redisClient *c, void *p, size_t len) {
    _addReplyLongLong(c,len,'$');
    addReplyString(c,p,len);
    addReplySds(c,shared.crlf);
}

/* Add a C nul term string as bulk reply */
void addReplyBulkCString(redisClient *c, char *s) {
    if (s == NULL) {
        addReplySds(c,shared.nullbulk);
    } else {
        addReplyBulkCBuffer(c,s,strlen(s));
    }
}

/* Add a long long as a bulk reply */
void addReplyBulkLongLong(redisClient *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}

static void acceptCommonHandler(int fd) {
    redisClient *c;
    if ((c = createClient(fd)) == NULL) {
        redisLog(REDIS_WARNING,"Error allocating resoures for the client");
        close(fd); /* May be already closed, just ingore errors */
        return;
    }
    /* If maxclient directive is set and this is one client more... close the
     * connection. Note that we create the client instead to check before
     * for this condition, since now the socket is already set in nonblocking
     * mode and we can send an error for free using the Kernel I/O */
    if (server.maxclients && listLength(server.clients) > server.maxclients) {
        char *err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors */
        if (write(c->fd,err,strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        freeClient(c);
        return;
    }
    server.stat_numconnections++;
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    char cip[128];
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    cfd = anetTcpAccept(server.neterr, fd, cip, &cport);
    if (cfd == AE_ERR) {
        redisLog(REDIS_VERBOSE,"Accepting client connection: %s", server.neterr);
        return;
    }
    redisLog(REDIS_VERBOSE,"Accepted %s:%d", cip, cport);
    acceptCommonHandler(cfd);
}

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cfd;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    cfd = anetUnixAccept(server.neterr, fd);
    if (cfd == AE_ERR) {
        redisLog(REDIS_VERBOSE,"Accepting client connection: %s", server.neterr);
        return;
    }
    redisLog(REDIS_VERBOSE,"Accepted connection to %s", server.unixsocket);
    acceptCommonHandler(cfd);
}


static void freeClientArgv(redisClient *c) {
    int j;
    for (j = 0; j < c->argc; j++)
        sdsfree(c->argv[j]);
    c->argc = 0;
}

void freeClient(redisClient *c) {
    listNode *ln;

    /* Note that if the client we are freeing is blocked into a blocking
     * call, we have to set querybuf to NULL *before* to call
     * unblockClientWaitingData() to avoid processInputBuffer() will get
     * called. Also it is important to remove the file events after
     * this, because this call adds the READABLE event. */
    sdsfree(c->querybuf);
    c->querybuf = NULL;
    if (c->flags & REDIS_BLOCKED)
        unblockClientWaitingData(c);

    /* Obvious cleanup */
    aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
    aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    listRelease(c->reply);
    freeClientArgv(c);
    close(c->fd);
    /* Remove from the list of clients */
    ln = listSearchKey(server.clients,c);
    redisAssert(ln != NULL);
    listDelNode(server.clients,ln);
    /* When client was just unblocked because of a blocking operation,
     * remove it from the list with unblocked clients. */
    if (c->flags & REDIS_UNBLOCKED) {
        ln = listSearchKey(server.unblocked_clients,c);
        redisAssert(ln != NULL);
        listDelNode(server.unblocked_clients,ln);
    }
    /* Remove from the list of clients waiting for swapped keys, or ready
     * to be restarted, but not yet woken up again. */
    if (c->flags & REDIS_IO_WAIT) {
        redisAssert(server.ds_enabled);
        if (listLength(c->io_keys) == 0) {
            ln = listSearchKey(server.io_ready_clients,c);

            /* When this client is waiting to be woken up (REDIS_IO_WAIT),
             * it should be present in the list io_ready_clients */
            redisAssert(ln != NULL);
            listDelNode(server.io_ready_clients,ln);
        } else {
            while (listLength(c->io_keys)) {
                ln = listFirst(c->io_keys);
                //TODO: FIX LATER
                //dontWaitForSwappedKey(c,ln->value);
            }
        }
        server.cache_blocked_clients--;
    }
    listRelease(c->io_keys);
    /* Release memory */
    zfree(c->argv);
    zfree(c);
}

void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = privdata;
    int nwritten = 0, totwritten = 0, len;
    sds s;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    while(c->bufpos > 0 || listLength(c->reply)) {
        if (c->bufpos > 0) {
            if (c->flags & REDIS_MASTER) {
                /* Don't reply to a master */
                nwritten = c->bufpos - c->sentlen;
            } else {
                nwritten = write(fd,c->buf+c->sentlen,c->bufpos-c->sentlen);
                if (nwritten <= 0) break;
            }
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            if (c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }
        } else {
            s = listNodeValue(listFirst(c->reply));
            len = sdslen(s);

            if (len == 0) {
                listDelNode(c->reply,listFirst(c->reply));
                continue;
            }

            if (c->flags & REDIS_MASTER) {
                /* Don't reply to a master */
                nwritten = len - c->sentlen;
            } else {
                nwritten = write(fd, ((char*)s)+c->sentlen,len-c->sentlen);
                if (nwritten <= 0) break;
            }
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If we fully sent the object on head go to the next one */
            if (c->sentlen == len) {
                listDelNode(c->reply,listFirst(c->reply));
                c->sentlen = 0;
            }
        }
        /* Note that we avoid to send more thank REDIS_MAX_WRITE_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interfae) */
        if (totwritten > REDIS_MAX_WRITE_PER_EVENT) break;
    }
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            redisLog(REDIS_VERBOSE,
                "Error writing to client: %s", strerror(errno));
            freeClient(c);
            return;
        }
    }
    if (totwritten > 0) c->lastinteraction = time(NULL);
    if (listLength(c->reply) == 0) {
        c->sentlen = 0;
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);

        /* Close connection after entire reply has been sent. */
        if (c->flags & REDIS_CLOSE_AFTER_REPLY) freeClient(c);
    }
}

/* resetClient prepare the client to process the next command */
void resetClient(redisClient *c) {
    freeClientArgv(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;
}

void closeTimedoutClients(void) {
    redisClient *c;
    listNode *ln;
    time_t now = time(NULL);
    listIter li;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);
        if (server.maxidletime &&
            !(c->flags & REDIS_BLOCKED) &&  /* no timeout for BLPOP */
            (now - c->lastinteraction > server.maxidletime))
        {
            redisLog(REDIS_VERBOSE,"Closing idle client");
            freeClient(c);
        } else if (c->flags & REDIS_BLOCKED) {
            if (c->bpop.timeout != 0 && c->bpop.timeout < now) {
                addReplySds(c,shared.nullmultibulk);
                unblockClientWaitingData(c);
            }
        }
    }
}

int processInlineBuffer(redisClient *c) {
    char *newline = strstr(c->querybuf,"\r\n");
    int argc;
    sds *argv;
    size_t querylen;

    /* Nothing to do without a \r\n */
    if (newline == NULL)
        return REDIS_ERR;

    /* Split the input buffer up to the \r\n */
    querylen = newline-(c->querybuf);
    argv = sdssplitlen(c->querybuf,querylen," ",1,&argc);

    c->argc = argc;

    c->argv = argv;

    /* Leave data after the first line of the query in the buffer */
    c->querybuf = sdsrange(c->querybuf,querylen+2,-1);

    return REDIS_OK;
}

/* Helper function. Trims query buffer to make the function that processes
 * multi bulk requests idempotent. */
static void setProtocolError(redisClient *c, int pos) {
    c->flags |= REDIS_CLOSE_AFTER_REPLY;
    c->querybuf = sdsrange(c->querybuf,pos,-1);
}

int processMultibulkBuffer(redisClient *c) {
    char *newline = NULL;
    char *eptr;
    int pos = 0, tolerr;
    long bulklen;

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        redisAssert(c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strstr(c->querybuf,"\r\n");
        if (newline == NULL)
            return REDIS_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        redisAssert(c->querybuf[0] == '*');
        c->multibulklen = strtol(c->querybuf+1,&eptr,10);
        pos = (newline-c->querybuf)+2;
        if (c->multibulklen <= 0) {
            c->querybuf = sdsrange(c->querybuf,pos,-1);
            return REDIS_OK;
        } else if (c->multibulklen > 1024*1024) {
            addReplyError(c,"Protocol error: invalid multibulk length");
            setProtocolError(c,pos);
            return REDIS_ERR;
        }

        /* Setup argv array on client structure */
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(sds)*c->multibulklen);

        /* Search new newline */
        newline = strstr(c->querybuf+pos,"\r\n");
    }

    redisAssert(c->multibulklen > 0);
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strstr(c->querybuf+pos,"\r\n");
            if (newline != NULL) {
                if (c->querybuf[pos] != '$') {
                    addReplyErrorFormat(c,
                        "Protocol error: expected '$', got '%c'",
                        c->querybuf[pos]);
                    setProtocolError(c,pos);
                    return REDIS_ERR;
                }

                bulklen = strtol(c->querybuf+pos+1,&eptr,10);
                tolerr = (eptr[0] != '\r');
                if (tolerr || bulklen == LONG_MIN || bulklen == LONG_MAX ||
                    bulklen < 0 || bulklen > 512*1024*1024)
                {
                    addReplyError(c,"Protocol error: invalid bulk length");
                    setProtocolError(c,pos);
                    return REDIS_ERR;
                }
                pos += eptr-(c->querybuf+pos)+2;
                c->bulklen = bulklen;
            } else {
                /* No newline in current buffer, so wait for more data */
                break;
            }
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-pos < (unsigned)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            c->argv[c->argc++] = sdsnewlen(c->querybuf+pos,c->bulklen);
            pos += c->bulklen+2;
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* Trim to pos */
    c->querybuf = sdsrange(c->querybuf,pos,-1);

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) {
        return REDIS_OK;
    }
    return REDIS_ERR;
}

void processInputBuffer(redisClient *c) {
    /* Keep processing while there is something in the input buffer */
    while(sdslen(c->querybuf)) {
        /* Immediately abort if the client is in the middle of something. */
        if (c->flags & REDIS_BLOCKED || c->flags & REDIS_IO_WAIT) return;

        /* REDIS_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands). */
        if (c->flags & REDIS_CLOSE_AFTER_REPLY) return;

        /* Determine request type when unknown. */
        if (!c->reqtype) {
            if (c->querybuf[0] == '*') {
                c->reqtype = REDIS_REQ_MULTIBULK;
            } else {
                c->reqtype = REDIS_REQ_INLINE;
            }
        }

        if (c->reqtype == REDIS_REQ_INLINE) {
            if (processInlineBuffer(c) != REDIS_OK) break;
        } else if (c->reqtype == REDIS_REQ_MULTIBULK) {
            if (processMultibulkBuffer(c) != REDIS_OK) break;
        } else {
            redisPanic("Unknown request type");
        }

        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            resetClient(c);
        } else {
            /* Only reset the client when the command was executed. */
            if (processCommand(c) == REDIS_OK)
                resetClient(c);
        }
    }
}

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = (redisClient*) privdata;
    char buf[REDIS_IOBUF_LEN];
    int nread;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    nread = read(fd, buf, REDIS_IOBUF_LEN);
    if (nread == -1) {
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            redisLog(REDIS_VERBOSE, "Reading from client: %s",strerror(errno));
            freeClient(c);
            return;
        }
    } else if (nread == 0) {
        redisLog(REDIS_VERBOSE, "Client closed connection");
        freeClient(c);
        return;
    }
    if (nread) {
        c->querybuf = sdscatlen(c->querybuf,buf,nread);
        c->lastinteraction = time(NULL);
    } else {
        return;
    }
    processInputBuffer(c);
}

void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer) {
    redisClient *c;
    listNode *ln;
    listIter li;
    unsigned long lol = 0, bib = 0;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);

        if (listLength(c->reply) > lol) lol = listLength(c->reply);
        if (sdslen(c->querybuf) > bib) bib = sdslen(c->querybuf);
    }
    *longest_output_list = lol;
    *biggest_input_buffer = bib;
}

