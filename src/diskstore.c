/* diskstore.c implements a very simple disk backed key-value store used
 * by Redis for the "disk" backend. This implementation uses the filesystem
 * to store key/value pairs. Every file represents a given key.
 *
 * The key path is calculated using the SHA1 of the key name. For instance
 * the key "foo" is stored as a file name called:
 *
 *  /0b/ee/0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33
 *
 * The couples of characters from the hex output of SHA1 are also used
 * to locate two two levels of directories to store the file (as most
 * filesystems are not able to handle too many files in a single dir).
 *
 * In the end there are 65536 final directories (256 directories inside
 * every 256 top level directories), so that with 1 billion of files every
 * directory will contain in the average 15258 entires, that is ok with
 * most filesystems implementation.
 *
 * Note that since Redis supports multiple databases, the actual key name
 * is:
 *
 *  /0b/ee/<dbid>_0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33
 *
 *  so for instance if the key is inside DB 0:
 *
 *  /0b/ee/0_0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33
 *
 * The actaul implementation of this disk store is highly dependant to the
 * filesystem implementation itself. This implementation may be replaced by
 * a B+TREE implementation in future implementations.
 *
 * Data ok every key is serialized using the same format used for .rdb
 * serialization. Everything is serialized on every entry: key name,
 * ttl information in case of keys with an associated expire time, and the
 * serialized value itself.
 *
 * Because the format is the same of the .rdb files it is trivial to create
 * an .rdb file starting from this format just by mean of scanning the
 * directories and concatenating entries, with the sole addition of an
 * .rdb header at the start and the end-of-db opcode at the end.
 *
 * -------------------------------------------------------------------------
 */

#include "errdb.h"
#include "sha1.h"

#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>


/**
* From rdb.c
*/
static int rdbWriteRaw(FILE *fp, void *p, size_t len) {
    if (fp != NULL && fwrite(p,len,1,fp) == 0) return -1;
    return len;
}

int rdbSaveType(FILE *fp, unsigned char type) {
    return rdbWriteRaw(fp,&type,1);
}

int rdbSaveTime(FILE *fp, time_t t) {
    int32_t t32 = (int32_t) t;
    return rdbWriteRaw(fp,&t32,4);
}

/* check rdbLoadLen() comments for more info */
int rdbSaveLen(FILE *fp, uint32_t len) {
    unsigned char buf[2];
    int nwritten;

    if (len < (1<<6)) {
        /* Save a 6 bit len */
        buf[0] = (len&0xFF)|(REDIS_RDB_6BITLEN<<6);
        if (rdbWriteRaw(fp,buf,1) == -1) return -1;
        nwritten = 1;
    } else if (len < (1<<14)) {
        /* Save a 14 bit len */
        buf[0] = ((len>>8)&0xFF)|(REDIS_RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
        if (rdbWriteRaw(fp,buf,2) == -1) return -1;
        nwritten = 2;
    } else {
        /* Save a 32 bit len */
        buf[0] = (REDIS_RDB_32BITLEN<<6);
        if (rdbWriteRaw(fp,buf,1) == -1) return -1;
        len = htonl(len);
        if (rdbWriteRaw(fp,&len,4) == -1) return -1;
        nwritten = 1+4;
    }
    return nwritten;
}

/* Encode 'value' as an integer if possible (if integer will fit the
 * supported range). If the function sucessful encoded the integer
 * then the (up to 5 bytes) encoded representation is written in the
 * string pointed by 'enc' and the length is returned. Otherwise
 * 0 is returned. */
int rdbEncodeInteger(long long value, unsigned char *enc) {
    /* Finally check if it fits in our ranges */
    if (value >= -(1<<7) && value <= (1<<7)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;
    } else if (value >= -(1<<15) && value <= (1<<15)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT16;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    } else {
        return 0;
    }
}

/* String objects in the form "2391" "-100" without any space and with a
 * range of values that can fit in an 8, 16 or 32 bit signed value can be
 * encoded as integers to save space */
int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc) {
    long long value;
    char *endptr, buf[32];

    /* Check if it's possible to encode this value as a number */
    value = strtoll(s, &endptr, 10);
    if (endptr[0] != '\0') return 0;
    ll2string(buf,32,value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    if (strlen(buf) != len || memcmp(buf,s,len)) return 0;

    return rdbEncodeInteger(value,enc);
}

/* Save a string objet as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form */
int rdbSaveRawString(FILE *fp, unsigned char *s, size_t len) {
    int enclen;
    int n, nwritten = 0;

    /* Try integer encoding */
    if (len <= 11) {
        unsigned char buf[5];
        if ((enclen = rdbTryIntegerEncoding((char*)s,len,buf)) > 0) {
            if (rdbWriteRaw(fp,buf,enclen) == -1) return -1;
            return enclen;
        }
    }

    /* Store verbatim */
    if ((n = rdbSaveLen(fp,len)) == -1) return -1;
    nwritten += n;
    if (len > 0) {
        if (rdbWriteRaw(fp,s,len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

/* Save a long long value as either an encoded string or a string. */
int rdbSaveLongLongAsStringObject(FILE *fp, long long value) {
    unsigned char buf[32];
    int n, nwritten = 0;
    int enclen = rdbEncodeInteger(value,buf);
    if (enclen > 0) {
        return rdbWriteRaw(fp,buf,enclen);
    } else {
        /* Encode as string */
        enclen = ll2string((char*)buf,32,value);
        redisAssert(enclen < 32);
        if ((n = rdbSaveLen(fp,enclen)) == -1) return -1;
        nwritten += n;
        if ((n = rdbWriteRaw(fp,buf,enclen)) == -1) return -1;
        nwritten += n;
    }
    return nwritten;
}

/* Like rdbSaveStringObjectRaw() but handle encoded objects */
int rdbSaveStringObject(FILE *fp, robj *obj) {
    /* Avoid to decode the object, then encode it again, if the
     * object is alrady integer encoded. */
    if (obj->encoding == REDIS_ENCODING_INT) {
        return rdbSaveLongLongAsStringObject(fp,(long)obj->ptr);
    } else {
        redisAssert(obj->encoding == REDIS_ENCODING_RAW);
        return rdbSaveRawString(fp,obj->ptr,sdslen(obj->ptr));
    }
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifing the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
int rdbSaveDoubleValue(FILE *fp, double val) {
    unsigned char buf[128];
    int len;

    if (isnan(val)) {
        buf[0] = 253;
        len = 1;
    } else if (!isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (val > min && val < max && val == ((double)((long long)val)))
            ll2string((char*)buf+1,sizeof(buf),(long long)val);
        else
#endif
            snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }
    return rdbWriteRaw(fp,buf,len);
}

/* Save a Redis object. */
int rdbSaveObject(FILE *fp, robj *o) {
    int n, nwritten = 0;

    if (o->type == REDIS_STRING) {
        /* Save a string value */
        if ((n = rdbSaveStringObject(fp,o)) == -1) return -1;
        nwritten += n;
    } else if (o->type == REDIS_LIST) {
        /* Save a list value */
        if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
            list *list = o->ptr;
            listIter li;
            listNode *ln;

            if ((n = rdbSaveLen(fp,listLength(list))) == -1) return -1;
            nwritten += n;

            listRewind(list,&li);
            while((ln = listNext(&li))) {
                robj *eleobj = listNodeValue(ln);
                if ((n = rdbSaveStringObject(fp,eleobj)) == -1) return -1;
                nwritten += n;
            }
        } else {
            redisPanic("Unknown list encoding");
        }
    } else {
        redisPanic("Unknown object type");
    }
    return nwritten;
}

/* Return the length the object will have on disk if saved with
 * the rdbSaveObject() function. Currently we use a trick to get
 * this length with very little changes to the code. In the future
 * we could switch to a faster solution. */
off_t rdbSavedObjectLen(robj *o) {
    int len = rdbSaveObject(NULL,o);
    redisAssert(len != -1);
    return len;
}

/* Save a key-value pair, with expire time, type, key, value.
 * On error -1 is returned.
 * On success if the key was actaully saved 1 is returned, otherwise 0
 * is returned (the key was already expired). */
int rdbSaveKeyValuePair(FILE *fp, robj *key, robj *val,
                        time_t expiretime, time_t now)
{
    int vtype;

    /* Save the expire time */
    if (expiretime != -1) {
        /* If this key is already expired skip it */
        if (expiretime < now) return 0;
        if (rdbSaveType(fp,REDIS_EXPIRETIME) == -1) return -1;
        if (rdbSaveTime(fp,expiretime) == -1) return -1;
    }
    /* Fix the object type if needed, to support saving zipmaps, ziplists,
     * and intsets, directly as blobs of bytes: they are already serialized. */
    vtype = val->type;
    /* Save type, key, value */
    if (rdbSaveType(fp,vtype) == -1) return -1;
    if (rdbSaveStringObject(fp,key) == -1) return -1;
    if (rdbSaveObject(fp,val) == -1) return -1;
    return 1;
}

/* Save the DB on disk. Return REDIS_ERR on error, REDIS_OK on success */
int rdbSave(char *filename) {

    if (server.ds_enabled) {
        cacheForcePointInTime();
        return dsRdbSave(filename);
    }

    return REDIS_OK;

}

int rdbSaveBackground(char *filename) {
    pid_t childpid;

    if (server.bgsavechildpid != -1 ||
        server.bgsavethread != (pthread_t) -1) return REDIS_ERR;

    server.dirty_before_bgsave = server.dirty;

    if (server.ds_enabled) {
        cacheForcePointInTime();
        return dsRdbSaveBackground(filename);
    }

    if ((childpid = fork()) == 0) {
        int retval;

        /* Child */
        if (server.ipfd > 0) close(server.ipfd);
        if (server.sofd > 0) close(server.sofd);
        retval = rdbSave(filename);
        _exit((retval == REDIS_OK) ? 0 : 1);
    } else {
        /* Parent */
        if (childpid == -1) {
            redisLog(REDIS_WARNING,"Can't save in background: fork: %s",
                strerror(errno));
            return REDIS_ERR;
        }
        redisLog(REDIS_NOTICE,"Background saving started by pid %d",childpid);
        server.bgsavechildpid = childpid;
        updateDictResizePolicy();
        return REDIS_OK;
    }
    return REDIS_OK; /* unreached */
}

void rdbRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,256,"temp-%d.rdb", (int) childpid);
    unlink(tmpfile);
}

int rdbLoadType(FILE *fp) {
    unsigned char type;
    if (fread(&type,1,1,fp) == 0) return -1;
    return type;
}

time_t rdbLoadTime(FILE *fp) {
    int32_t t32;
    if (fread(&t32,4,1,fp) == 0) return -1;
    return (time_t) t32;
}

/* Load an encoded length from the DB, see the REDIS_RDB_* defines on the top
 * of this file for a description of how this are stored on disk.
 *
 * isencoded is set to 1 if the readed length is not actually a length but
 * an "encoding type", check the above comments for more info */
uint32_t rdbLoadLen(FILE *fp, int *isencoded) {
    unsigned char buf[2];
    uint32_t len;
    int type;

    if (isencoded) *isencoded = 0;
    if (fread(buf,1,1,fp) == 0) return REDIS_RDB_LENERR;
    type = (buf[0]&0xC0)>>6;
    if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len */
        return buf[0]&0x3F;
    } else if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit len encoding type */
        if (isencoded) *isencoded = 1;
        return buf[0]&0x3F;
    } else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len */
        if (fread(buf+1,1,1,fp) == 0) return REDIS_RDB_LENERR;
        return ((buf[0]&0x3F)<<8)|buf[1];
    } else {
        /* Read a 32 bit len */
        if (fread(&len,4,1,fp) == 0) return REDIS_RDB_LENERR;
        return ntohl(len);
    }
}

/* Load an integer-encoded object from file 'fp', with the specified
 * encoding type 'enctype'. If encode is true the function may return
 * an integer-encoded object as reply, otherwise the returned object
 * will always be encoded as a raw string. */
robj *rdbLoadIntegerObject(FILE *fp, int enctype, int encode) {
    unsigned char enc[4];
    long long val;

    if (enctype == REDIS_RDB_ENC_INT8) {
        if (fread(enc,1,1,fp) == 0) return NULL;
        val = (signed char)enc[0];
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (fread(enc,2,1,fp) == 0) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (fread(enc,4,1,fp) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        val = 0; /* anti-warning */
        redisPanic("Unknown RDB integer encoding type");
    }
    if (encode)
        return createStringObjectFromLongLong(val);
    else
        return createObject(REDIS_STRING,sdsfromlonglong(val));
}

robj *rdbGenericLoadStringObject(FILE*fp, int encode) {
    int isencoded;
    uint32_t len;
    sds val;

    len = rdbLoadLen(fp,&isencoded);
    if (isencoded) {
        switch(len) {
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            return rdbLoadIntegerObject(fp,len,encode);
        default:
            redisPanic("Unknown RDB encoding type");
        }
    }

    if (len == REDIS_RDB_LENERR) return NULL;
    val = sdsnewlen(NULL,len);
    if (len && fread(val,len,1,fp) == 0) {
        sdsfree(val);
        return NULL;
    }
    return createObject(REDIS_STRING,val);
}

robj *rdbLoadStringObject(FILE *fp) {
    return rdbGenericLoadStringObject(fp,0);
}

robj *rdbLoadEncodedStringObject(FILE *fp) {
    return rdbGenericLoadStringObject(fp,1);
}

/* For information about double serialization check rdbSaveDoubleValue() */
int rdbLoadDoubleValue(FILE *fp, double *val) {
    char buf[128];
    unsigned char len;

    if (fread(&len,1,1,fp) == 0) return -1;
    switch(len) {
    case 255: *val = R_NegInf; return 0;
    case 254: *val = R_PosInf; return 0;
    case 253: *val = R_Nan; return 0;
    default:
        if (fread(buf,len,1,fp) == 0) return -1;
        buf[len] = '\0';
        sscanf(buf, "%lg", val);
        return 0;
    }
}

/* Load a Redis object of the specified type from the specified file.
 * On success a newly allocated object is returned, otherwise NULL. */
robj *rdbLoadObject(int type, FILE *fp) {
    robj *o, *ele;
    size_t len;

    redisLog(REDIS_DEBUG,"LOADING OBJECT %d (at %d)\n",type,ftell(fp));
    if (type == REDIS_STRING) {
        /* Read string value */
        o = rdbLoadEncodedStringObject(fp);
    } else if (type == REDIS_LIST) {
        /* Read list value */
        if ((len = rdbLoadLen(fp,NULL)) == REDIS_RDB_LENERR) return NULL;

        o = createListObject();

        /* Load every single element of the list */
        while(len--) {
            if ((ele = rdbLoadEncodedStringObject(fp)) == NULL) return NULL;
            listAddNodeTail(o->ptr,ele);
        }
    } else {
        redisPanic("Unknown object type");
    }
    return o;
}

/* Mark that we are loading in the global state and setup the fields
 * needed to provide loading stats. */
void startLoading(FILE *fp) {
}

/* Refresh the loading progress info */
void loadingProgress(off_t pos) {
}

/* Loading finished */
void stopLoading(void) {
}

/* A background saving child (BGSAVE) terminated its work. Handle this. */
void backgroundSaveDoneHandler(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        redisLog(REDIS_NOTICE,
            "Background saving terminated with success");
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = time(NULL);
    } else if (!bysignal && exitcode != 0) {
        redisLog(REDIS_WARNING, "Background saving error");
    } else {
        redisLog(REDIS_WARNING,
            "Background saving terminated by signal %d", bysignal);
        rdbRemoveTempFile(server.bgsavechildpid);
    }
    server.bgsavechildpid = -1;
    server.bgsavethread = (pthread_t) -1;
}

void saveCommand(redisClient *c) {
    if (server.bgsavechildpid != -1 || server.bgsavethread != (pthread_t)-1) {
        addReplyError(c,"Background save already in progress");
        return;
    }
    if (rdbSave(server.dbfilename) == REDIS_OK) {
        addReply(c,shared.ok);
    } else {
        addReply(c,shared.err);
    }
}

void bgsaveCommand(redisClient *c) {
    if (server.bgsavechildpid != -1 || server.bgsavethread != (pthread_t)-1) {
        addReplyError(c,"Background save already in progress");
        return;
    }
    if (rdbSaveBackground(server.dbfilename) == REDIS_OK) {
        addReplyStatus(c,"Background saving started");
    } else {
        addReply(c,shared.err);
    }
}



int create256dir(char *prefix) {
    char buf[1024];
    int j;

    for (j = 0; j < 256; j++) {
        snprintf(buf,sizeof(buf),"%s%02x",prefix,j);
        if (mkdir(buf,0755) == -1) {
            redisLog(REDIS_WARNING,"Error creating dir %s for diskstore: %s",
                buf,strerror(errno));
            return REDIS_ERR;
        }
    }
    return REDIS_OK;
}

int dsOpen(void) {
    struct stat sb;
    int retval, j;
    char *path = server.ds_path;
    char buf[1024];

    if ((retval = stat(path,&sb) == -1) && errno != ENOENT) {
        redisLog(REDIS_WARNING, "Error opening disk store at %s: %s",
                path, strerror(errno));
        return REDIS_ERR;
    }

    /* Directory already in place. Assume everything is ok. */
    if (retval == 0 && S_ISDIR(sb.st_mode)) {
        redisLog(REDIS_NOTICE,"Disk store %s exists", path);
        return REDIS_OK;
    }

    /* File exists but it's not a directory */
    if (retval == 0 && !S_ISDIR(sb.st_mode)) {
        redisLog(REDIS_WARNING,"Disk store at %s is not a directory", path);
        return REDIS_ERR;
    }

    /* New disk store, create the directory structure now, as creating
     * them in a lazy way is not a good idea, after very few insertions
     * we'll need most of the 65536 directories anyway. */
    redisLog(REDIS_NOTICE,"Disk store %s does not exist: creating", path);
    if (mkdir(path,0755) == -1) {
        redisLog(REDIS_WARNING,"Disk store init failed creating dir %s: %s",
            path, strerror(errno));
        return REDIS_ERR;
    }
    /* Create the top level 256 directories */
    snprintf(buf,sizeof(buf),"%s/",path);
    if (create256dir(buf) == REDIS_ERR) return REDIS_ERR;

    /* For every 256 top level dir, create 256 nested dirs */
    for (j = 0; j < 256; j++) {
        snprintf(buf,sizeof(buf),"%s/%02x/",path,j);
        if (create256dir(buf) == REDIS_ERR) return REDIS_ERR;
    }
    return REDIS_OK;
}

int dsClose(void) {
    return REDIS_OK;
}

/* Convert key into full path for this object. Dirty but hopefully
 * is fast enough. Returns the length of the returned path. */
int dsKeyToPath(redisDb *db, char *buf, robj *key) {
    SHA1_CTX ctx;
    unsigned char hash[20];
    char hex[40], digits[] = "0123456789abcdef";
    int j, l;
    char *origbuf = buf;

    SHA1Init(&ctx);
    SHA1Update(&ctx,key->ptr,sdslen(key->ptr));
    SHA1Final(hash,&ctx);

    /* Convert the hash into hex format */
    for (j = 0; j < 20; j++) {
        hex[j*2] = digits[(hash[j]&0xF0)>>4];
        hex[(j*2)+1] = digits[hash[j]&0x0F];
    }

    /* Create the object path. Start with server.ds_path that's the root dir */
    l = sdslen(server.ds_path);
    memcpy(buf,server.ds_path,l);
    buf += l;
    *buf++ = '/';

    /* Then add xx/yy/ that is the two level directories */
    buf[0] = hex[0];
    buf[1] = hex[1];
    buf[2] = '/';
    buf[3] = hex[2];
    buf[4] = hex[3];
    buf[5] = '/';
    buf += 6;

    /* Add the database number followed by _ and finall the SHA1 hex */
    l = ll2string(buf,64,db->id);
    buf += l;
    buf[0] = '_';
    memcpy(buf+1,hex,40);
    buf[41] = '\0';
    return (buf-origbuf)+41;
}

int dsSet(redisDb *db, robj *key, robj *val, time_t expire) {
    char buf[1024], buf2[1024];
    FILE *fp;
    int retval, len;

    len = dsKeyToPath(db,buf,key);
    memcpy(buf2,buf,len);
    snprintf(buf2+len,sizeof(buf2)-len,"-%ld-%ld",(long)time(NULL),(long)val);
    while ((fp = fopen(buf2,"w")) == NULL) {
        if (errno == ENOSPC) {
            redisLog(REDIS_WARNING,"Diskstore: No space left on device. Please make room and wait 30 seconds for Redis to continue.");
            sleep(30);
        } else {
            redisLog(REDIS_WARNING,"diskstore error opening %s: %s",
                buf2, strerror(errno));
            redisPanic("Unrecoverable diskstore error. Exiting.");
        }
    }
    if ((retval = rdbSaveKeyValuePair(fp,key,val,expire,time(NULL))) == -1)
        return REDIS_ERR;
    fclose(fp);
    if (retval == 0) {
        /* Expired key. Unlink failing not critical */
        unlink(buf);
        unlink(buf2);
    } else {
        /* Use rename for atomic updadte of value */
        if (rename(buf2,buf) == -1) {
            redisLog(REDIS_WARNING,"rename(2) returned an error: %s",
                strerror(errno));
            redisPanic("Unrecoverable diskstore error. Exiting.");
        }
    }
    return REDIS_OK;
}

robj *dsGet(redisDb *db, robj *key, time_t *expire) {
    char buf[1024];
    int type;
    time_t expiretime = -1; /* -1 means: no expire */
    robj *dskey; /* Key as loaded from disk. */
    robj *val;
    FILE *fp;

    dsKeyToPath(db,buf,key);
    fp = fopen(buf,"r");
    if (fp == NULL && errno == ENOENT) return NULL; /* No such key */
    if (fp == NULL) {
        redisLog(REDIS_WARNING,"Disk store failed opening %s: %s",
            buf, strerror(errno));
        goto readerr;
    }

    if ((type = rdbLoadType(fp)) == -1) goto readerr;
    if (type == REDIS_EXPIRETIME) {
        //if ((expiretime = rdbLoadTime(fp)) == -1) goto readerr;
        /* We read the time so we need to read the object type again */
        //if ((type = rdbLoadType(fp)) == -1) goto readerr;
    }
    /* Read key */
    if ((dskey = rdbLoadStringObject(fp)) == NULL) goto readerr;
    /* Read value */
    if ((val = rdbLoadObject(type,fp)) == NULL) goto readerr;
    fclose(fp);

    /* The key we asked, and the key returned, must be the same */
    redisAssert(equalStringObjects(key,dskey));

    /* Check if the key already expired */
    decrRefCount(dskey);
    if (expiretime != -1 && expiretime < time(NULL)) {
        decrRefCount(val);
        unlink(buf); /* This failing is non critical here */
        return NULL;
    }

    /* Everything ok... */
    *expire = expiretime;
    return val;

readerr:
    redisLog(REDIS_WARNING,"Read error reading reading %s. Corrupted key?",
        buf);
    redisPanic("Unrecoverable error reading from disk store");
    return NULL; /* unreached */
}

int dsDel(redisDb *db, robj *key) {
    char buf[1024];

    dsKeyToPath(db,buf,key);
    if (unlink(buf) == -1) {
        if (errno == ENOENT) {
            return REDIS_ERR;
        } else {
            redisLog(REDIS_WARNING,"Disk store can't remove %s: %s",
                buf, strerror(errno));
            redisPanic("Unrecoverable Disk store errore. Existing.");
            return REDIS_ERR; /* unreached */
        }
    } else {
        return REDIS_OK;
    }
}

int dsExists(redisDb *db, robj *key) {
    char buf[1024];

    dsKeyToPath(db,buf,key);
    return access(buf,R_OK) == 0;
}

int dsGetDbidFromFilename(char *path) {
    char id[64];
    char *p = strchr(path,'_');
    int len = (p - path);

    redisAssert(p != NULL && len < 64);
    memcpy(id,path,len);
    id[len] = '\0';
    return atoi(id);
}

void dsFlushOneDir(char *path, int dbid) {
    DIR *dir;
    struct dirent *dp, de;

    dir = opendir(path);
    if (dir == NULL) {
        redisLog(REDIS_WARNING,"Disk store can't open dir %s: %s",
            path, strerror(errno));
        redisPanic("Unrecoverable Disk store errore. Existing.");
    }
    while(1) {
        char buf[1024];

        readdir_r(dir,&de,&dp);
        if (dp == NULL) break;
        if (dp->d_name[0] == '.') continue;

        /* Check if we need to remove this entry accordingly to the
         * DB number. */
        if (dbid != -1 && dsGetDbidFromFilename(dp->d_name)) continue;
        
        /* Finally unlink the file */
        snprintf(buf,1024,"%s/%s",path,dp->d_name);
        if (unlink(buf) == -1) {
            redisLog(REDIS_WARNING,
                "Can't unlink %s: %s", buf, strerror(errno));
            redisPanic("Unrecoverable Disk store errore. Existing.");
        }
    }
    closedir(dir);
}

void dsFlushDb(int dbid) {
    char buf[1024];
    int j, i;

    redisLog(REDIS_NOTICE,"Flushing diskstore DB (%d)",dbid);
    for (j = 0; j < 256; j++) {
        for (i = 0; i < 256; i++) {
            snprintf(buf,1024,"%s/%02x/%02x",server.ds_path,j,i);
            dsFlushOneDir(buf,dbid);
        }
    }
}

void dsRdbSaveSetState(int state) {
    pthread_mutex_lock(&server.bgsavethread_mutex);
    server.bgsavethread_state = state;
    pthread_mutex_unlock(&server.bgsavethread_mutex);
}

void *dsRdbSave_thread(void *arg) {
    char tmpfile[256], *filename = (char*)arg;
    struct dirent *dp, de;
    int j, i, last_dbid = -1;
    FILE *fp;

    /* Change state to ACTIVE, to signal there is a saving thead working. */
    redisLog(REDIS_NOTICE,"Diskstore BGSAVE thread started");
    dsRdbSaveSetState(REDIS_BGSAVE_THREAD_ACTIVE);

    snprintf(tmpfile,256,"temp-%d.rdb", (int) getpid());
    fp = fopen(tmpfile,"w");
    if (!fp) {
        redisLog(REDIS_WARNING, "Failed opening .rdb for saving: %s",
            strerror(errno));
        dsRdbSaveSetState(REDIS_BGSAVE_THREAD_DONE_ERR);
        return NULL;
    }
    if (fwrite("REDIS0001",9,1,fp) == 0) goto werr;

    sleep(5);

    /* Scan all diskstore dirs looking for keys */
    for (j = 0; j < 256; j++) {
        for (i = 0; i < 256; i++) {
            DIR *dir;
            char buf[1024];

            /* For every directory, collect all the keys */
            snprintf(buf,sizeof(buf),"%s/%02x/%02x",server.ds_path,j,i);
            if ((dir = opendir(buf)) == NULL) {
                redisLog(REDIS_WARNING,"Disk store can't open dir %s: %s",
                    buf, strerror(errno));
                goto werr;
            }

            while(1) {
                char buf[1024];
                int dbid;
                FILE *entryfp;

                readdir_r(dir,&de,&dp);
                if (dp == NULL) break;
                if (dp->d_name[0] == '.') continue;
                /* If there is a '-' char in the file name, it's a temp file */
                if (strchr(dp->d_name,'-') != NULL) continue;

                /* Emit the SELECT DB opcode if needed. */
                dbid = dsGetDbidFromFilename(dp->d_name);
                if (dbid != last_dbid) {
                    last_dbid = dbid;
                    if (rdbSaveType(fp,REDIS_SELECTDB) == -1) goto werr;
                    if (rdbSaveLen(fp,dbid) == -1) goto werr;
                }

                /* Let's copy this file into the target .rdb */
                snprintf(buf,sizeof(buf),"%s/%02x/%02x/%s",
                    server.ds_path,j,i,dp->d_name);
                if ((entryfp = fopen(buf,"r")) == NULL) {
                    redisLog(REDIS_WARNING,"Can't open %s: %s",
                        buf,strerror(errno));
                    closedir(dir);
                    goto werr;
                }
                while(1) {
                    int nread = fread(buf,1,sizeof(buf),entryfp);

                    if (nread == 0) {
                        if (ferror(entryfp)) {
                            redisLog(REDIS_WARNING,"Error reading from file entry while performing BGSAVE for diskstore: %s", strerror(errno));
                            closedir(dir);
                            goto werr;
                        } else {
                            break;
                        }
                    }
                    if (fwrite(buf,1,nread,fp) != (unsigned)nread) {
                        closedir(dir);
                        goto werr;
                    }
                }
                fclose(entryfp);
            }
            closedir(dir);
        }
    }
    
    /* Output the end of file opcode */
    if (rdbSaveType(fp,REDIS_EOF) == -1) goto werr;

    /* Make sure data will not remain on the OS's output buffers */
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);
    zfree(filename);

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    if (rename(tmpfile,filename) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp DB file on the final destination: %s (diskstore)", strerror(errno));
        unlink(tmpfile);
        dsRdbSaveSetState(REDIS_BGSAVE_THREAD_DONE_ERR);
        return NULL;
    }
    redisLog(REDIS_NOTICE,"DB saved on disk by diskstore thread");
    dsRdbSaveSetState(REDIS_BGSAVE_THREAD_DONE_OK);
    return NULL;

werr:
    zfree(filename);
    fclose(fp);
    unlink(tmpfile);
    dsRdbSaveSetState(REDIS_BGSAVE_THREAD_DONE_ERR);
    redisLog(REDIS_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    return NULL;
}

int dsRdbSaveBackground(char *filename) {
    pthread_t thread;

    if (pthread_create(&thread,NULL,dsRdbSave_thread,zstrdup(filename)) != 0) {
        redisLog(REDIS_WARNING,"Can't create diskstore BGSAVE thread: %s",
            strerror(errno));
        return REDIS_ERR;
    } else {
        server.bgsavethread = thread;
        return REDIS_OK;
    }
}

int dsRdbSave(char *filename) {
    /* A blocking save is actually a non blocking save... just we wait
     * for it to terminate in a non-busy loop. */

    redisLog(REDIS_NOTICE,"Starting a blocking SAVE (BGSAVE + blocking wait)");
    server.dirty_before_bgsave = server.dirty;
    if (dsRdbSaveBackground(filename) == REDIS_ERR) return REDIS_ERR;
    while(1) {
        usleep(1000);
        int state;

        pthread_mutex_lock(&server.bgsavethread_mutex);
        state = server.bgsavethread_state;
        pthread_mutex_unlock(&server.bgsavethread_mutex);

        if (state == REDIS_BGSAVE_THREAD_DONE_OK ||
            state == REDIS_BGSAVE_THREAD_DONE_ERR) break;
    }
    return REDIS_OK;
}
