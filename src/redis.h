#ifndef __ERRDB_H
#define __ERRDB_H

#include "fmacros.h"
#include "config.h"

#if defined(__sun)
#include "solarisfixes.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>

#include "ae.h"     /* Event driven programming library */
#include "sds.h"    /* Dynamic safe strings */
#include "dict.h"   /* Hash tables */
#include "adlist.h" /* Linked lists */
#include "zmalloc.h" /* total memory usage aware version of malloc/free */
#include "anet.h"   /* Networking the easy way */
#include "version.h"

/* Error codes */
#define REDIS_OK                0
#define REDIS_ERR               -1

/* Static server configuration */
#define REDIS_SERVERPORT        6379    /* TCP port */
#define REDIS_MAXIDLETIME       (60*5)  /* default client timeout */
#define REDIS_IOBUF_LEN         1024
#define REDIS_LOADBUF_LEN       1024
#define REDIS_STATIC_ARGS       8
#define REDIS_DEFAULT_DBNUM     16
#define REDIS_CONFIGLINE_MAX    1024
#define REDIS_MAX_SYNC_TIME     60      /* Slave can't take more to sync */
#define REDIS_EXPIRELOOKUPS_PER_CRON    10 /* lookup 10 expires per loop */
#define REDIS_MAX_WRITE_PER_EVENT (1024*64)
#define REDIS_REQUEST_MAX_SIZE (1024*1024*256) /* max bytes in inline command */
#define REDIS_SHARED_INTEGERS 10000
#define REDIS_REPLY_CHUNK_BYTES (5*1500) /* 5 TCP packets with default MTU */
#define REDIS_MAX_LOGMSG_LEN    1024 /* Default maximum length of syslog messages */

/* Hash table parameters */
#define REDIS_HT_MINFILL        10      /* Minimal hash table fill 10% */

/* Command flags:
 *   REDIS_CMD_DENYOOM:
 *     Commands marked with this flag will return an error when 'maxmemory' is
 *     set and the server is using more than 'maxmemory' bytes of memory.
 *     In short: commands with this flag are denied on low memory conditions.
 *   REDIS_CMD_FORCE_REPLICATION:
 *     Force replication even if dirty is 0. */
#define REDIS_CMD_DENYOOM 4
#define REDIS_CMD_FORCE_REPLICATION 8

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
#define REDIS_ENCODING_RAW 0     /* Raw representation */
#define REDIS_ENCODING_INT 1     /* Encoded as integer */
#define REDIS_ENCODING_HT 2      /* Encoded as hash table */
#define REDIS_ENCODING_LINKEDLIST 4 /* Encoded as regular linked list */
#define REDIS_ENCODING_TS 8 /* Encoded as time series data*/

/* Object types only used for dumping to disk */
#define REDIS_EXPIRETIME 253
#define REDIS_SELECTDB 254
#define REDIS_EOF 255

/* Defines related to the dump file format. To store 32 bits lengths for short
 * keys requires a lot of space, so we check the most significant 2 bits of
 * the first byte to interpreter the length:
 *
 * 00|000000 => if the two MSB are 00 the len is the 6 bits of this byte
 * 01|000000 00000000 =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
 * 10|000000 [32 bit integer] => if it's 01, a full 32 bit len will follow
 * 11|000000 this means: specially encoded object will follow. The six bits
 *           number specify the kind of object that follows.
 *           See the REDIS_RDB_ENC_* defines.
 *
 * Lenghts up to 63 are stored using a single byte, most DB keys, and may
 * values, will fit inside. */
#define REDIS_RDB_6BITLEN 0
#define REDIS_RDB_14BITLEN 1
#define REDIS_RDB_32BITLEN 2
#define REDIS_RDB_ENCVAL 3
#define REDIS_RDB_LENERR UINT_MAX

/* When a length of a string object stored on disk has the first two bits
 * set, the remaining two bits specify a special encoding for the object
 * accordingly to the following defines: */
#define REDIS_RDB_ENC_INT8 0        /* 8 bit signed integer */
#define REDIS_RDB_ENC_INT16 1       /* 16 bit signed integer */
#define REDIS_RDB_ENC_INT32 2       /* 32 bit signed integer */
#define REDIS_RDB_ENC_LZF 3         /* string compressed with FASTLZ */

/* Scheduled IO opeations flags. */
#define REDIS_IO_LOAD 1
#define REDIS_IO_SAVE 2
#define REDIS_IO_LOADINPROG 4
#define REDIS_IO_SAVEINPROG 8

/* Generic IO flags */
#define REDIS_IO_ONLYLOADS 1
#define REDIS_IO_ASAP 2

#define REDIS_MAX_COMPLETED_JOBS_PROCESSED 1
#define REDIS_THREAD_STACK_SIZE (1024*1024*4)

/* Client flags */
#define REDIS_SLAVE 1       /* This client is a slave server */
#define REDIS_MASTER 2      /* This client is a master server */
#define REDIS_MONITOR 4     /* This client is a slave monitor, see MONITOR */
#define REDIS_MULTI 8       /* This client is in a MULTI context */
#define REDIS_BLOCKED 16    /* The client is waiting in a blocking operation */
#define REDIS_IO_WAIT 32    /* The client is waiting for Virtual Memory I/O */
#define REDIS_DIRTY_CAS 64  /* Watched keys modified. EXEC will fail. */
#define REDIS_CLOSE_AFTER_REPLY 128 /* Close after writing entire reply. */
#define REDIS_UNBLOCKED 256 /* This client was unblocked and is stored in
                               server.unblocked_clients */

/* Client request types */
#define REDIS_REQ_INLINE 1
#define REDIS_REQ_MULTIBULK 2

/* Slave replication state - slave side */
#define REDIS_REPL_NONE 0   /* No active replication */
#define REDIS_REPL_CONNECT 1    /* Must connect to master */
#define REDIS_REPL_TRANSFER 2    /* Receiving .rdb from master */
#define REDIS_REPL_CONNECTED 3  /* Connected to master */

/* Slave replication state - from the point of view of master
 * Note that in SEND_BULK and ONLINE state the slave receives new updates
 * in its output queue. In the WAIT_BGSAVE state instead the server is waiting
 * to start the next background saving in order to send updates to it. */
#define REDIS_REPL_WAIT_BGSAVE_START 3 /* master waits bgsave to start feeding it */
#define REDIS_REPL_WAIT_BGSAVE_END 4 /* master waits bgsave to start bulk DB transmission */
#define REDIS_REPL_SEND_BULK 5 /* master is sending the bulk DB */
#define REDIS_REPL_ONLINE 6 /* bulk DB already transmitted, receive updates */

/* List related stuff */
#define REDIS_HEAD 0
#define REDIS_TAIL 1

/* Sort operations */
#define REDIS_SORT_GET 0
#define REDIS_SORT_ASC 1
#define REDIS_SORT_DESC 2
#define REDIS_SORTKEY_MAX 1024

/* Log levels */
#define REDIS_DEBUG 0
#define REDIS_VERBOSE 1
#define REDIS_NOTICE 2
#define REDIS_WARNING 3

/* Anti-warning macro... */
#define REDIS_NOTUSED(V) ((void) V)

#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^32 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */

/* Append only defines */
#define APPENDFSYNC_NO 0
#define APPENDFSYNC_ALWAYS 1
#define APPENDFSYNC_EVERYSEC 2

/* Sets operations codes */
#define REDIS_OP_UNION 0
#define REDIS_OP_DIFF 1
#define REDIS_OP_INTER 2

/* Redis maxmemory strategies */
#define REDIS_MAXMEMORY_VOLATILE_LRU 0
#define REDIS_MAXMEMORY_VOLATILE_TTL 1
#define REDIS_MAXMEMORY_VOLATILE_RANDOM 2
#define REDIS_MAXMEMORY_ALLKEYS_LRU 3
#define REDIS_MAXMEMORY_ALLKEYS_RANDOM 4
#define REDIS_MAXMEMORY_NO_EVICTION 5

/* Diskstore background saving thread states */
#define REDIS_BGSAVE_THREAD_UNACTIVE 0
#define REDIS_BGSAVE_THREAD_ACTIVE 1
#define REDIS_BGSAVE_THREAD_DONE_OK 2
#define REDIS_BGSAVE_THREAD_DONE_ERR 3

/* We can print the stacktrace, so our assert is defined this way: */
#define redisAssert(_e) ((_e)?(void)0 : (_redisAssert(#_e,__FILE__,__LINE__),_exit(1)))
#define redisPanic(_e) _redisPanic(#_e,__FILE__,__LINE__),_exit(1)
void _redisAssert(char *estr, char *file, int line);
void _redisPanic(char *msg, char *file, int line);

/*-----------------------------------------------------------------------------
 * Data types
 *----------------------------------------------------------------------------*/

/* A redis object, that is a type able to hold a string / list / set */

/* The actual Redis Object */
#define REDIS_LRU_CLOCK_MAX ((1<<21)-1) /* Max value of obj->lru */
#define REDIS_LRU_CLOCK_RESOLUTION 10 /* LRU clock resolution in seconds */
typedef struct tsObject {
    unsigned tag:2;
    unsigned time:30;
    sds value;
} tsObj;

/* Macro used to initalize a Redis object allocated on the stack.
 * Note that this macro is taken near the structure definition to make sure
 * we'll update it when the structure is changed, to avoid bugs like
 * bug #85 introduced exactly in this way. */
#define initStaticStringObject(_var,_ptr) do { \
    _var.refcount = 1; \
    _var.type = REDIS_STRING; \
    _var.encoding = REDIS_ENCODING_RAW; \
    _var.ptr = _ptr; \
} while(0);

typedef struct redisDb {
    dict *dict;                 /* The keyspace for this DB */
    dict *expires;              /* Timeout of keys with a timeout set */
    dict *blocking_keys;        /* Keys with clients waiting for data (BLPOP) */
    dict *io_keys;              /* Keys with clients waiting for DS I/O */
    dict *io_negcache;          /* Negative caching for disk store */
    dict *io_queued;            /* Queued IO operations hash table */
    int id;
} redisDb;

typedef struct blockingState {
    sds *keys;            /* The key we are waiting to terminate a blocking
                             * operation such as BLPOP. Otherwise NULL. */
    int count;              /* Number of blocking keys */
    time_t timeout;         /* Blocking operation timeout. If UNIX current time
                             * is >= timeout then the operation timed out. */
    sds *target;           /* The key that should receive the element,
                             * for BRPOPLPUSH. */
} blockingState;

/* With multiplexing we need to take per-clinet state.
 * Clients are taken in a liked list. */
typedef struct redisClient {
    int fd;
    redisDb *db;
    int dictid;
    sds querybuf;
    int argc;
    sds *argv;
    int reqtype;
    int multibulklen;       /* number of multi bulk arguments left to read */
    long bulklen;           /* length of bulk argument in multi bulk request */
    list *reply;
    int sentlen;
    time_t lastinteraction; /* time of the last interaction, used for timeout */
    int flags;              /* REDIS_SLAVE | REDIS_MONITOR | REDIS_MULTI ... */
    int slaveseldb;         /* slave selected db, if this client is a slave */
    int authenticated;      /* when requirepass is non-NULL */
    int replstate;          /* replication state if this is a slave */
    int repldbfd;           /* replication DB file descriptor */
    long repldboff;         /* replication DB file offset */
    off_t repldbsize;       /* replication DB file size */
    blockingState bpop;   /* blocking state */
    list *io_keys;          /* Keys this client is waiting to be loaded from the
                             * swap file in order to continue. */

    /* Response buffer */
    int bufpos;
    char buf[REDIS_REPLY_CHUNK_BYTES];
} redisClient;

struct saveparam {
    time_t seconds;
    int changes;
};

struct sharedObjectsStruct {
    sds crlf, ok, err, emptybulk, czero, cone, cnegone, pong, space,
    colon, nullbulk, nullmultibulk, queued,
    emptymultibulk, wrongtypeerr, nokeyerr, syntaxerr, sameobjecterr,
    outofrangeerr, loadingerr, plus,
    messagebulk, pmessagebulk, mbulk3,
    mbulk4 ;
};

/* Global server state structure */
struct redisServer {
    /* General */
    pthread_t mainthread;
    redisDb *db;
    dict *commands;             /* Command table hahs table */
    aeEventLoop *el;
    /* Networking */
    int port;
    char *bindaddr;
    char *unixsocket;
    int ipfd;
    int sofd;
    list *clients;
    list *monitors;
    char neterr[ANET_ERR_LEN];
    /* Fast pointers to often looked up command */
    struct redisCommand *delCommand, *multiCommand;
    int cronloops;              /* number of times the cron function run */
    time_t lastsave;                /* Unix time of last save succeeede */
    /* Fields used only for stats */
    time_t stat_starttime;          /* server start time */
    long long stat_numcommands;     /* number of processed commands */
    long long stat_numconnections;  /* number of connections received */
    long long stat_expiredkeys;     /* number of expired keys */
    long long stat_evictedkeys;     /* number of evicted keys (maxmemory) */
    long long stat_keyspace_hits;   /* number of successful lookups of keys */
    long long stat_keyspace_misses; /* number of failed lookups of keys */
    /* Configuration */
    int verbosity;
    int maxidletime;
    int daemonize;
    int shutdown_asap;
    int activerehashing;
    char *requirepass;
    /* Persistence */
    long long dirty;            /* changes to DB from the last save */
    long long dirty_before_bgsave; /* used to restore dirty on failed BGSAVE */
    time_t lastfsync;
    int appendfd;
    int appendseldb;
    char *pidfile;
    pid_t bgsavechildpid;
    pid_t bgrewritechildpid;
    int bgsavethread_state;
    pthread_mutex_t bgsavethread_mutex;
    pthread_t bgsavethread;
    struct saveparam *saveparams;
    int saveparamslen;
    char *dbfilename;
    int rdbcompression;
    /* Logging */
    char *logfile;
    int syslog_enabled;
    char *syslog_ident;
    int syslog_facility;
    /* Limits */
    unsigned int maxclients;
    unsigned long long maxmemory;
    int maxmemory_policy;
    int maxmemory_samples;
    /* Blocked clients */
    unsigned int bpop_blocked_clients;
    unsigned int cache_blocked_clients;
    list *unblocked_clients; /* list of clients to unblock before next loop */
    list *cache_io_queue;    /* IO operations queue */
    int cache_flush_delay;   /* seconds to wait before flushing keys */
    /* Sort parameters - qsort_r() is only available under BSD so we
     * have to take this state global, in order to pass it to sortCompare() */
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
    /* Virtual memory configuration */
    int ds_enabled; /* backend disk in redis.conf */
    char *ds_path;  /* location of the disk store on disk */
    unsigned long long cache_max_memory;
    /* Zip structure config */
    size_t list_max_ziplist_entries;
    size_t list_max_ziplist_value;
    size_t set_max_intset_entries;
    time_t unixtime;    /* Unix time sampled every second. */
    /* Virtual memory I/O threads stuff */
    /* An I/O thread process an element taken from the io_jobs queue and
     * put the result of the operation in the io_done list. While the
     * job is being processed, it's put on io_processing queue. */
    list *io_newjobs; /* List of VM I/O jobs yet to be processed */
    list *io_processing; /* List of VM I/O jobs being processed */
    list *io_processed; /* List of VM I/O jobs already processed */
    list *io_ready_clients; /* Clients ready to be unblocked. All keys loaded */
    pthread_mutex_t io_mutex; /* lock to access io_jobs/io_done/io_thread_job */
    pthread_cond_t io_condvar; /* I/O threads conditional variable */
    pthread_attr_t io_threads_attr; /* attributes for threads creation */
    int io_active_threads; /* Number of running I/O threads */
    int vm_max_threads; /* Max number of I/O threads running at the same time */
    /* Our main thread is blocked on the event loop, locking for sockets ready
     * to be read or written, so when a threaded I/O operation is ready to be
     * processed by the main thread, the I/O thread will use a unix pipe to
     * awake the main thread. The followings are the two pipe FDs. */
    int io_ready_pipe_read;
    int io_ready_pipe_write;
    /* Misc */
    unsigned lruclock:22;        /* clock incrementing every minute, for LRU */
    unsigned lruclock_padding:10;
};

typedef void redisCommandProc(redisClient *c);
struct redisCommand {
    char *name;
    redisCommandProc *proc;
    int arity;
    int flags;
    /* What keys should be loaded in background when calling this command? */
    int vm_firstkey; /* The first argument that's a key (0 = no keys) */
    int vm_lastkey;  /* THe last argument that's a key */
    int vm_keystep;  /* The step between first and last key */
    long long microseconds, calls;
};

struct redisFunctionSym {
    char *name;
    unsigned long pointer;
};

/* DIsk store threaded I/O request message */
#define REDIS_IOJOB_LOAD 0
#define REDIS_IOJOB_SAVE 1

typedef struct iojob {
    int type;   /* Request type, REDIS_IOJOB_* */
    redisDb *db;/* Redis database */
    sds key;  /* This I/O request is about this key */
    sds val;  /* the value to swap for REDIS_IOJOB_SAVE, otherwise this
                 * field is populated by the I/O thread for REDIS_IOJOB_LOAD. */
    time_t expire; /* Expire time for this key on REDIS_IOJOB_LOAD */
} iojob;

/* IO operations scheduled -- check dscache.c for more info */
typedef struct ioop {
    int type;
    redisDb *db;
    sds key;
    time_t ctime; /* This is the creation time of the entry. */
} ioop;

/* Structure to hold list iteration abstraction. */
typedef struct {
    list *subject;
    unsigned char direction; /* Iteration direction */
    unsigned char *zi;
    listNode *ln;
} listTypeIterator;

/* Structure for an entry while iterating over a list. */
typedef struct {
    listTypeIterator *li;
    unsigned char *zi;  /* Entry in ziplist */
    listNode *ln;       /* Entry in linked list */
} listTypeEntry;


/* Structure to hold hash iteration abstration. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
typedef struct {
    int encoding;
    unsigned char *zi;
    unsigned char *zk, *zv;
    unsigned int zklen, zvlen;

    dictIterator *di;
    dictEntry *de;
} hashTypeIterator;

#define REDIS_HASH_KEY 1
#define REDIS_HASH_VALUE 2

/*-----------------------------------------------------------------------------
 * Extern declarations
 *----------------------------------------------------------------------------*/

extern struct redisServer server;
extern struct sharedObjectsStruct shared;
extern dictType setDictType;
extern dictType zsetDictType;
extern double R_Zero, R_PosInf, R_NegInf, R_Nan;
dictType hashDictType;

/*-----------------------------------------------------------------------------
 * Functions prototypes
 *----------------------------------------------------------------------------*/

/* Utils */
long long ustime(void);

/* networking.c -- Networking and Client related operations */
redisClient *createClient(int fd);
void closeTimedoutClients(void);
void freeClient(redisClient *c);
void resetClient(redisClient *c);
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask);
void *addDeferredMultiBulkLength(redisClient *c);
void setDeferredMultiBulkLength(redisClient *c, void *node, long length);
void addReplySds(redisClient *c, sds s);
void processInputBuffer(redisClient *c);
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
void addReplyBulk(redisClient *c, sds s);
void addReplyBulkCString(redisClient *c, char *s);
void addReplyBulkCBuffer(redisClient *c, void *p, size_t len);
void addReplyBulkLongLong(redisClient *c, long long ll);
void acceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
//void addReply(redisClient *c, robj *obj);
void addReplySds(redisClient *c, sds s);
void addReplyObj(redisClient *c, tsObj *obj);
void addReplyError(redisClient *c, char *err);
void addReplyStatus(redisClient *c, char *status);
void addReplyDouble(redisClient *c, double d);
void addReplyLongLong(redisClient *c, long long ll);
void addReplyMultiBulkLen(redisClient *c, long length);
void *dupClientReplyValue(void *o);
void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer);

#ifdef __GNUC__
void addReplyErrorFormat(redisClient *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
void addReplyStatusFormat(redisClient *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void addReplyErrorFormat(redisClient *c, const char *fmt, ...);
void addReplyStatusFormat(redisClient *c, const char *fmt, ...);
#endif

/* Synchronous I/O with timeout */
int syncWrite(int fd, char *ptr, ssize_t size, int timeout);
int syncRead(int fd, char *ptr, ssize_t size, int timeout);
int syncReadLine(int fd, char *ptr, ssize_t size, int timeout);
int fwriteBulkString(FILE *fp, char *s, unsigned long len);
int fwriteBulkDouble(FILE *fp, double d);
int fwriteBulkLongLong(FILE *fp, long long l);
int fwriteBulkObject(FILE *fp, sds obj);

/* Generic persistence functions */
void startLoading(FILE *fp);
void loadingProgress(off_t pos);
void stopLoading(void);

/* Core functions */
void freeMemoryIfNeeded(void);
int processCommand(redisClient *c);
void setupSignalHandlers(void);
struct redisCommand *lookupCommand(sds name);
struct redisCommand *lookupCommandByCString(char *s);
void call(redisClient *c, struct redisCommand *cmd);
int prepareForShutdown();
void redisLog(int level, const char *fmt, ...);
void usage();
void updateDictResizePolicy(void);
int htNeedsResize(dict *dict);
void oom(const char *msg);
void populateCommandTable(void);
void resetCommandTableStats(void);

/* Utility functions */
int stringmatchlen(const char *pattern, int patternLen,
        const char *string, int stringLen, int nocase);
int stringmatch(const char *pattern, const char *string, int nocase);
long long memtoll(const char *p, int *err);
int ll2string(char *s, size_t len, long long value);
int isStringRepresentableAsLong(sds s, long *longval);
int isStringRepresentableAsLongLong(sds s, long long *longval);

/* Configuration */
void loadServerConfig(char *filename);
void appendServerSaveParams(time_t seconds, int changes);
void resetServerSaveParams();

/* db.c -- Keyspace access API */
int removeExpire(redisDb *db, sds key);
void propagateExpire(redisDb *db, sds key);
int expireIfNeeded(redisDb *db, sds key);
time_t getExpire(redisDb *db, sds key);
void setExpire(redisDb *db, sds key, time_t when);
list *lookupKey(redisDb *db, sds key);
list *lookupKeyRead(redisDb *db, sds key);
list *lookupKeyWrite(redisDb *db, sds key);
int dbAdd(redisDb *db, sds key, list *val);
int dbReplace(redisDb *db, sds key, list *val);
int dbExists(redisDb *db, sds key);
sds dbRandomKey(redisDb *db);
int dbDelete(redisDb *db, sds key);
long long emptyDb();
int selectDb(redisClient *c, int id);
void signalModifiedKey(redisDb *db, sds key);
void signalFlushedDb(int dbid);

/* Git SHA1 */
char *redisGitSHA1(void);
char *redisGitDirty(void);

/* Commands prototypes */
void authCommand(redisClient *c);
void pingCommand(redisClient *c);
void echoCommand(redisClient *c);
void setCommand(redisClient *c);
void setnxCommand(redisClient *c);
void setexCommand(redisClient *c);
void getCommand(redisClient *c);
void delCommand(redisClient *c);
void existsCommand(redisClient *c);
void setbitCommand(redisClient *c);
void getbitCommand(redisClient *c);
void setrangeCommand(redisClient *c);
void getrangeCommand(redisClient *c);
void incrCommand(redisClient *c);
void decrCommand(redisClient *c);
void incrbyCommand(redisClient *c);
void decrbyCommand(redisClient *c);
void selectCommand(redisClient *c);
void randomkeyCommand(redisClient *c);
void keysCommand(redisClient *c);
void dbsizeCommand(redisClient *c);
void lastsaveCommand(redisClient *c);
void saveCommand(redisClient *c);
void bgsaveCommand(redisClient *c);
void bgrewriteaofCommand(redisClient *c);
void shutdownCommand(redisClient *c);
void moveCommand(redisClient *c);
void renameCommand(redisClient *c);
void renamenxCommand(redisClient *c);
void lpushCommand(redisClient *c);
void rpushCommand(redisClient *c);
void lpushxCommand(redisClient *c);
void rpushxCommand(redisClient *c);
//Time series data
void tsInsertCommand(redisClient *c);
void tsFetchCommand(redisClient *c);
void tsLastCommand(redisClient *c);

void linsertCommand(redisClient *c);
void lpopCommand(redisClient *c);
void rpopCommand(redisClient *c);
void llenCommand(redisClient *c);
void lindexCommand(redisClient *c);
void lrangeCommand(redisClient *c);
void ltrimCommand(redisClient *c);
void typeCommand(redisClient *c);
void lsetCommand(redisClient *c);
void syncCommand(redisClient *c);
void flushallCommand(redisClient *c);
void lremCommand(redisClient *c);
void rpoplpushCommand(redisClient *c);
void infoCommand(redisClient *c);
void mgetCommand(redisClient *c);
void monitorCommand(redisClient *c);
void expireCommand(redisClient *c);
void expireatCommand(redisClient *c);
void getsetCommand(redisClient *c);
void ttlCommand(redisClient *c);
void persistCommand(redisClient *c);
void slaveofCommand(redisClient *c);
void msetCommand(redisClient *c);
void msetnxCommand(redisClient *c);
void multiCommand(redisClient *c);
void execCommand(redisClient *c);
void discardCommand(redisClient *c);
void blpopCommand(redisClient *c);
void brpopCommand(redisClient *c);
void brpoplpushCommand(redisClient *c);
void appendCommand(redisClient *c);
void strlenCommand(redisClient *c);
void configCommand(redisClient *c);
void watchCommand(redisClient *c);
void unwatchCommand(redisClient *c);

#if defined(__GNUC__)
void *calloc(size_t count, size_t size) __attribute__ ((deprecated));
void free(void *ptr) __attribute__ ((deprecated));
void *malloc(size_t size) __attribute__ ((deprecated));
void *realloc(void *ptr, size_t size) __attribute__ ((deprecated));
#endif

#endif
