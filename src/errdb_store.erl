%%%----------------------------------------------------------------------
%%% File    : errdb_store.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : File Storage 
%%% Created : 03 Apr. 2010
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2012, www.opengoss.com
%%%----------------------------------------------------------------------
-module(errdb_store).

-author('ery.lee@gmail.com').

-include_lib("elog/include/elog.hrl").

-import(lists, [concat/1, reverse/1]).

-import(proplists, [get_value/3]).

-import(extbif, [zeropad/1, timestamp/0, datetime/1,strfdate/1]).

-export([start_link/2,
		read/5,
        write/2]).

-behavior(gen_server).

-export([init/1, 
        handle_call/3, 
        priorities_call/3,
        handle_cast/2,
        handle_info/2,
        priorities_info/2,
        terminate/2,
        code_change/3]).

-define(SCHEMA, "CREATE TABLE metrics ("
				"object TEXT, time INTEGER, "
				"metric TEXT, value REAL);").

-define(INDEX, "CREATE UNIQUE INDEX object_time_metric_idx on "
			   "metrics(object, time, metric);").

-define(PRAGMA, "pragma synchronous=normal;").

-define(ATTACH(File), ["attach '", File, "' as hourly;"]).

-define(IMPORT, "insert into metrics(object,time,metric,value) "
				"select object,time,metric,value from hourly.metrics").

%db0: hour db
%db1: today db
%db2: yesterday db
-record(state, {id, dir, db0, db1, db2}).

start_link(Id, Dir) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, 
		[Id, Dir], [{spawn_opt, [{min_heap_size, 204800}]}]).

name(Id) ->
    list_to_atom("errdb_store_" ++ integer_to_list(Id)).

read(Pid, Object, Fields, Begin, End) ->
	gen_server2:call(Pid, {read, Object, Fields, Begin, End}).

%Record: {Object, Timestamp, Metrics}
write(Pid, Records) ->
	gen_server2:cast(Pid, {insert, Records}).

init([Id, Dir]) ->
	{ok, DB0} = opendb(hourly, Id, Dir),
	{ok, DB1} = opendb(today, Id, Dir),
	{ok, DB2} = opendb(yesterday, Id, Dir),
	sched_next_hourly_commit(),
	sched_next_daily_commit(),
	?INFO("~p is started.", [name(Id)]),
	{ok, #state{id = Id, dir = Dir,
		db0=DB0, db1=DB1, db2=DB2}}.

opendb(hourly, Id, Dir) ->
	File = concat([Dir, "/", strfdate(today()), 
		"/", zeropad(hour()), "/", dbfile(Id)]),
	opendb(dbname("hourly", Id), File);

opendb(today, Id, Dir) ->
	File = concat([Dir, "/", strfdate(today()), "/", dbfile(Id)]),
	opendb(dbname("today", Id), File);

opendb(yesterday, Id, Dir) ->
	File = concat([Dir, "/", strfdate(yesterday()), "/", dbfile(Id)]),
	opendb(dbname("yesterday", Id), File).
	
opendb(Name, File) ->
	?INFO("opendb: ~p", [File]),
	filelib:ensure_dir(File),
	{ok, DB} = sqlite3:open(Name, [{file, File}]),
	schema(DB, sqlite3:list_tables(DB)),
	{ok, DB}.

schema(DB, []) ->
	sqlite3:sql_exec(DB, ?PRAGMA),
	sqlite3:sql_exec(DB, ?SCHEMA),
	sqlite3:sql_exec(DB, ?INDEX);

schema(_DB, [metrics]) ->
	ok.

dbname(Prefix, Id) when is_list(Prefix) ->
	list_to_atom(Prefix ++ zeropad(Id)).

dbfile(Id) ->
	integer_to_list(Id) ++ ".db".

handle_call({read, Object, Fields, Begin, End}, _From, 
	#state{db0 = DB0, db1 = DB1, db2 = DB2} = State) ->
	%?INFO("read: ~p, ~p, ~p, ~p", [Object, Fields, Begin, End]),
	SQL = ["select time, metric, value from metrics "
		   "where object = '", Object, "' and metric in ",
		    "(", string:join(["'"++F++"'" || F <- Fields], ","), ")"
			" and time >= ", integer_to_list(Begin), 
			" and time <= ", integer_to_list(End), ";"],
	Results = [sqlite3:sql_exec(DB, SQL) || DB <- [DB2, DB1, DB0]], 
	Rows = lists:flatten([Rows || [{columns, _}, {rows, Rows}] <- Results]),
	Reply = {ok, transform([list_to_binary(F)|| F <- Fields], Rows)},
    {reply, Reply, State};
	
handle_call(_Req, _From, State) ->
    {reply, {error, badreq}, State}.

priorities_call({read, _Object, _Fields, _Begin, _End}, _From, _State) ->
    10;
priorities_call(_, _From, _State) ->
    0.

handle_cast({insert, Records}, #state{db0= DB0} = State) ->
	Rows =
	lists:foldl(fun({Object, Time, Metrics}, Acc) ->
		Row = fun(Metric, Value) ->
			[{object, Object}, {time, Time},
			 {metric, Metric}, {value, Value}]
		end,
		Rows = [Row(Metric, Value) || {Metric, Value} <- Metrics],
		Rows ++ Acc
	end, [], Records),
	sqlite3:write_many(DB0, metrics, Rows),
	{noreply, State};

handle_cast(Msg, State) ->
    {stop, {error, {badmsg, Msg}}, State}.

handle_info({commit, hourly}, #state{id = Id, dir = Dir, db0 = DB0, db1 = DB1} = State) ->
	sched_next_hourly_commit(),
	File = sqlite3:dbfile(DB0),
	case filelib:is_file(File) of
	true ->
		spawn(fun() -> 
			sqlite3:sql_exec(DB1, ?ATTACH(File)),
			sqlite3:sql_exec(DB1, ?IMPORT)
		end);
	false ->
		ignore
	end,
	sqlite3:close(DB0),
	{ok, NewDB0} = opendb(hourly, Id, Dir),
	{noreply, State#state{db0 = NewDB0}};

handle_info({commit, daily}, #state{id = Id, dir = Dir, db1 = DB1, db2 = DB2} = State) ->
	sched_next_daily_commit(),
	sqlite3:close(DB2),
	{ok, NewDB1} = opendb(today, Id, Dir),
	{noreply, State#state{db1 = NewDB1, db2 = DB1}};
	
handle_info(Info, State) ->
    {stop, {error, {badinfo, Info}}, State}.

priorities_info({commit, hourly}, _) ->
	11;
priorities_info({commit, daily}, _) ->
	10;
priorities_info(_, _) ->
    1.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

today() -> date().

hour() -> {H,_,_} = time(), H.

yesterday() -> {Date, _} = datetime(timestamp() - 86400), Date.

sched_next_hourly_commit() ->
	Ts1 = timestamp(),
    Ts2 = (Ts1 div 3600 + 1) * 3600,
	Diff = (Ts2 + 1 - Ts1) * 1000,
    erlang:send_after(Diff, self(), {commit, hourly}).

sched_next_daily_commit() ->
	Ts1 = timestamp(),
    Ts2 = (Ts1 div 86400 + 1) * 86400,
	Diff = (Ts2 + 60 - Ts1) * 1000,
    erlang:send_after(Diff, self(), {commit, daily}).

transform(Fields, Rows) ->
	TimeDict = 
	lists:foldl(fun({Time, Metric, Value}, Dict) -> 
		case orddict:find(Time, Dict) of
		{ok, Metrics} -> orddict:store(Time, [{Metric, Value}|Metrics], Dict);
		error -> orddict:store(Time, [{Metric, Value}], Dict)
		end	
	end, orddict:new(), Rows),
	Values = fun(Metrics) -> 
		[get_value(Name, Metrics, "NaN") || Name <- Fields]
	end,
	[{Time, Values(Metrics)} || {Time, Metrics} <- orddict:to_list(TimeDict)].

