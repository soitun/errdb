#include "redis.h"

/*-----------------------------------------------------------------------------
 * Config file parsing
 *----------------------------------------------------------------------------*/

int yesnotoi(char *s) {
    if (!strcasecmp(s,"yes")) return 1;
    else if (!strcasecmp(s,"no")) return 0;
    else return -1;
}

void appendServerSaveParams(time_t seconds, int changes) {
    server.saveparams = zrealloc(server.saveparams,sizeof(struct saveparam)*(server.saveparamslen+1));
    server.saveparams[server.saveparamslen].seconds = seconds;
    server.saveparams[server.saveparamslen].changes = changes;
    server.saveparamslen++;
}

void resetServerSaveParams() {
    zfree(server.saveparams);
    server.saveparams = NULL;
    server.saveparamslen = 0;
}

/* I agree, this is a very rudimental way to load a configuration...
   will improve later if the config gets more complex */
void loadServerConfig(char *filename) {
    FILE *fp;
    char buf[REDIS_CONFIGLINE_MAX+1], *err = NULL;
    int linenum = 0;
    sds line = NULL;

    if (filename[0] == '-' && filename[1] == '\0')
        fp = stdin;
    else {
        if ((fp = fopen(filename,"r")) == NULL) {
            redisLog(REDIS_WARNING, "Fatal error, can't open config file '%s'", filename);
            exit(1);
        }
    }

    while(fgets(buf,REDIS_CONFIGLINE_MAX+1,fp) != NULL) {
        sds *argv;
        int argc, j;

        linenum++;
        line = sdsnew(buf);
        line = sdstrim(line," \t\r\n");

        /* Skip comments and blank lines*/
        if (line[0] == '#' || line[0] == '\0') {
            sdsfree(line);
            continue;
        }

        /* Split into arguments */
        argv = sdssplitargs(line,&argc);
        sdstolower(argv[0]);

        /* Execute config directives */
        if (!strcasecmp(argv[0],"timeout") && argc == 2) {
            server.maxidletime = atoi(argv[1]);
            if (server.maxidletime < 0) {
                err = "Invalid timeout value"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"port") && argc == 2) {
            server.port = atoi(argv[1]);
            if (server.port < 0 || server.port > 65535) {
                err = "Invalid port"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"bind") && argc == 2) {
            server.bindaddr = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"unixsocket") && argc == 2) {
            server.unixsocket = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"save") && argc == 3) {
            int seconds = atoi(argv[1]);
            int changes = atoi(argv[2]);
            if (seconds < 1 || changes < 0) {
                err = "Invalid save parameters"; goto loaderr;
            }
            appendServerSaveParams(seconds,changes);
        } else if (!strcasecmp(argv[0],"dir") && argc == 2) {
            if (chdir(argv[1]) == -1) {
                redisLog(REDIS_WARNING,"Can't chdir to '%s': %s",
                    argv[1], strerror(errno));
                exit(1);
            }
        } else if (!strcasecmp(argv[0],"loglevel") && argc == 2) {
            if (!strcasecmp(argv[1],"debug")) server.verbosity = REDIS_DEBUG;
            else if (!strcasecmp(argv[1],"verbose")) server.verbosity = REDIS_VERBOSE;
            else if (!strcasecmp(argv[1],"notice")) server.verbosity = REDIS_NOTICE;
            else if (!strcasecmp(argv[1],"warning")) server.verbosity = REDIS_WARNING;
            else {
                err = "Invalid log level. Must be one of debug, notice, warning";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"logfile") && argc == 2) {
            FILE *logfp;

            server.logfile = zstrdup(argv[1]);
            if (!strcasecmp(server.logfile,"stdout")) {
                zfree(server.logfile);
                server.logfile = NULL;
            }
            if (server.logfile) {
                /* Test if we are able to open the file. The server will not
                 * be able to abort just for this problem later... */
                logfp = fopen(server.logfile,"a");
                if (logfp == NULL) {
                    err = sdscatprintf(sdsempty(),
                        "Can't open the log file: %s", strerror(errno));
                    goto loaderr;
                }
                fclose(logfp);
            }
        } else if (!strcasecmp(argv[0],"syslog-enabled") && argc == 2) {
            if ((server.syslog_enabled = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"syslog-ident") && argc == 2) {
            if (server.syslog_ident) zfree(server.syslog_ident);
            server.syslog_ident = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"syslog-facility") && argc == 2) {
            struct {
                const char     *name;
                const int       value;
            } validSyslogFacilities[] = {
                {"user",    LOG_USER},
                {"local0",  LOG_LOCAL0},
                {"local1",  LOG_LOCAL1},
                {"local2",  LOG_LOCAL2},
                {"local3",  LOG_LOCAL3},
                {"local4",  LOG_LOCAL4},
                {"local5",  LOG_LOCAL5},
                {"local6",  LOG_LOCAL6},
                {"local7",  LOG_LOCAL7},
                {NULL, 0}
            };
            int i;

            for (i = 0; validSyslogFacilities[i].name; i++) {
                if (!strcasecmp(validSyslogFacilities[i].name, argv[1])) {
                    server.syslog_facility = validSyslogFacilities[i].value;
                    break;
                }
            }

            if (!validSyslogFacilities[i].name) {
                err = "Invalid log facility. Must be one of USER or between LOCAL0-LOCAL7";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"include") && argc == 2) {
            loadServerConfig(argv[1]);
        } else if (!strcasecmp(argv[0],"maxclients") && argc == 2) {
            server.maxclients = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"maxmemory") && argc == 2) {
            server.maxmemory = memtoll(argv[1],NULL);
        } else if (!strcasecmp(argv[0],"maxmemory-policy") && argc == 2) {
            if (!strcasecmp(argv[1],"volatile-lru")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_LRU;
            } else if (!strcasecmp(argv[1],"volatile-random")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_RANDOM;
            } else if (!strcasecmp(argv[1],"volatile-ttl")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_TTL;
            } else if (!strcasecmp(argv[1],"allkeys-lru")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_LRU;
            } else if (!strcasecmp(argv[1],"allkeys-random")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_RANDOM;
            } else if (!strcasecmp(argv[1],"noeviction")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_NO_EVICTION;
            } else {
                err = "Invalid maxmemory policy";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"maxmemory-samples") && argc == 2) {
            server.maxmemory_samples = atoi(argv[1]);
            if (server.maxmemory_samples <= 0) {
                err = "maxmemory-samples must be 1 or greater";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"glueoutputbuf")) {
            redisLog(REDIS_WARNING, "Deprecated configuration directive: \"%s\"", argv[0]);
        } else if (!strcasecmp(argv[0],"activerehashing") && argc == 2) {
            if ((server.activerehashing = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"daemonize") && argc == 2) {
            if ((server.daemonize = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"requirepass") && argc == 2) {
            server.requirepass = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"pidfile") && argc == 2) {
            zfree(server.pidfile);
            server.pidfile = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"dbfilename") && argc == 2) {
            zfree(server.dbfilename);
            server.dbfilename = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"diskstore-enabled") && argc == 2) {
            if ((server.ds_enabled = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"diskstore-path") && argc == 2) {
            sdsfree(server.ds_path);
            server.ds_path = sdsnew(argv[1]);
        } else if (!strcasecmp(argv[0],"cache-max-memory") && argc == 2) {
            server.cache_max_memory = memtoll(argv[1],NULL);
        } else if (!strcasecmp(argv[0],"cache-flush-delay") && argc == 2) {
            server.cache_flush_delay = atoi(argv[1]);
            if (server.cache_flush_delay < 0) server.cache_flush_delay = 0;
        } else if (!strcasecmp(argv[0],"list-max-ziplist-entries") && argc == 2){
            server.list_max_ziplist_entries = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"list-max-ziplist-value") && argc == 2) {
            server.list_max_ziplist_value = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"set-max-intset-entries") && argc == 2) {
            server.set_max_intset_entries = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"rename-command") && argc == 3) {
            struct redisCommand *cmd = lookupCommand(argv[1]);
            int retval;

            if (!cmd) {
                err = "No such command in rename-command";
                goto loaderr;
            }

            /* If the target command name is the emtpy string we just
             * remove it from the command table. */
            retval = dictDelete(server.commands, argv[1]);
            redisAssert(retval == DICT_OK);

            /* Otherwise we re-add the command under a different name. */
            if (sdslen(argv[2]) != 0) {
                sds copy = sdsdup(argv[2]);

                retval = dictAdd(server.commands, copy, cmd);
                if (retval != DICT_OK) {
                    sdsfree(copy);
                    err = "Target command name already exists"; goto loaderr;
                }
            }
        } else {
            err = "Bad directive or wrong number of arguments"; goto loaderr;
        }
        for (j = 0; j < argc; j++)
            sdsfree(argv[j]);
        zfree(argv);
        sdsfree(line);
    }
    if (fp != stdin) fclose(fp);
    return;

loaderr:
    fprintf(stderr, "\n*** FATAL CONFIG FILE ERROR ***\n");
    fprintf(stderr, "Reading the configuration file, at line %d\n", linenum);
    fprintf(stderr, ">>> '%s'\n", line);
    fprintf(stderr, "%s\n", err);
    exit(1);
}
