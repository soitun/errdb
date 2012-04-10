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


%hdbs: history databases
-record(state, {id, name, hour, dir, db, hdbs}).

start_link(Id, Dir) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, 
		[Id, Dir], [{spawn_opt, [{min_heap_size, 409600}]}]).

name(Id) ->
    list_to_atom("errdb_store_" ++ integer_to_list(Id)).

read(Pid, Object, Fields, Begin, End) ->
	gen_server2:call(Pid, {read, Object, Fields, Begin, End}).

write(Pid, Rows) ->
	gen_server2:cast(Pid, {write, Rows}).

init([Id, Dir]) ->
	Now = timestamp(),
	Hour = Now div 3600,
	DB = open(db, Dir, Now, Id),
	HDBS = [ open(hdb, Dir, ago(Now, I), Id)
				|| I <- lists:seq(1, 47) ],
	sched_next_hourly_rotate(),
	?INFO("~p is started.", [name(Id)]),
	{ok, #state{id = Id, name = name(Id), hour = Hour, 
		dir = Dir, db = DB, hdbs=HDBS}}.

open(Type, Dir, Ts, Id) ->
	{Date, {Hour,_,_}} = extbif:datetime(Ts),
	Name = dbname(Date, Hour, Id),
	File = dbfile(Dir, Date, Hour, Id),
	case {Type, filelib:is_file(File)} of
	{db, true} ->
		opendb(Name, File);
	{db, false} ->
		filelib:ensure_dir(File),
		opendb(Name, File);
	{hdb, true}->
		opendb(Name, File);
	{hdb, false} ->
		undefined
	end.

opendb(Name, File) ->
	error_logger:info_msg("opendb: ~p ~p~n", [Name, File]),
	{ok, DB} = sqlite3:open(Name, [{file, File}]),
	schema(DB, sqlite3:list_tables(DB)),
	DB.

dbname(Date, Hour, Id) ->
	list_to_atom(concat([strfdate(Date), zeropad(Hour), zeropad(Id)])).

dbfile(Dir, Date, Hour, Id) ->
	concat([Dir, "/", strfdate(Date), 
		"/", zeropad(Hour), "/", 
		integer_to_list(Id), ".db"]).

schema(DB, []) ->
	sqlite3:sql_exec(DB, "CREATE TABLE metrics ("
				"object TEXT, time INTEGER, "
				"metric TEXT, value REAL);"),
	sqlite3:sql_exec(DB, "CREATE UNIQUE INDEX object_time_metric_idx "
			   "on metrics(object, time, metric);");
	%sqlite3:sql_exec(DB, "pragma journal_mode=memory;"),
	%sqlite3:sql_exec(DB, "pragma synchronous=off;").

schema(_DB, [metrics]) ->
	ok.

ago(Now, I) -> Now - I*3600.

hourdelta(Hour, Begin, End) ->
	BeginHour = Begin div 3600,
	EndHour = End div 3600,
	
	EndDelta = Hour - EndHour,
	EndDelta1 =
	if
	EndDelta =< 0 -> 1;
	true -> EndDelta
	end,

	BeginDelta = Hour - BeginHour,
	BeginDelta1 = 
	if
	BeginDelta =< 0 -> 1;
	BeginDelta > 48 -> 48; %only two days
	BeginDelta -> BeginDelta
	end,
	{BeginDelta1, EndDelta1}.

handle_call({read, Object, Fields, Begin, End}, _From, 
	#state{hour = Hour, db = DB, hdbs = HDBS} = State) ->
	SQL = ["select time, metric, value from metrics "
		   "where object = '", Object, "' and metric in ",
		    "(", string:join(["'"++F++"'" || F <- Fields], ","), ")"
			" and time >= ", integer_to_list(Begin), 
			" and time <= ", integer_to_list(End), ";"],
	{BeginDelta, EndDelta} = hourdelta(Hour, Begin, End),
	?INFO("~p", [{BeginDelta, EndDelta}]),
	DbInRange = lists:sublist([DB|HDBS], EndDelta, BeginDelta - EndDelta + 1), 
	DbInRange1 = [D || D <- DbInRange, D =/= undefined],
	Results = pmap(fun(D) -> sqlite3:sql_exec(D, SQL) end, DbInRange1),
	Rows = lists:flatten([Rows || [{columns, _}, {rows, Rows}] <- Results]),
	Reply = {ok, Fields, Rows},
    {reply, Reply, State};
	
handle_call(_Req, _From, State) ->
    {reply, {error, badreq}, State}.

priorities_call({read, _Object, _Fields, _Begin, _End}, _From, _State) ->
    10;
priorities_call(_, _From, _State) ->
    0.

handle_cast({write, Rows}, #state{db = DB} = State) ->
	sqlite3:write_many(DB, metrics, Rows), 
	{noreply, State};

handle_cast(Msg, State) ->
    {stop, {error, {badmsg, Msg}}, State}.

handle_info(rotate, #state{id = Id, dir = Dir, db = DB, hdbs = HDBS} = State) ->
	sched_next_hourly_rotate(),
	Now = timestamp(),
	Hour = Now div 3600,
	NewDB = open(db, Dir, Now, Id), 
	close(lists:last(HDBS)),
	NewHDBS = [DB | lists:sublist(HDBS, 1, length(HDBS)-1)],
	{noreply, State#state{hour = Hour, db = NewDB, hdbs = NewHDBS}};

handle_info(Info, State) ->
    {stop, {error, {badinfo, Info}}, State}.

priorities_info(rotate, _) ->
	11;
priorities_info(_, _) ->
    1.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

sched_next_hourly_rotate() ->
	Ts1 = timestamp(),
    Ts2 = (Ts1 div 3600 + 1) * 3600,
	Diff = (Ts2 + 1 - Ts1) * 1000,
    erlang:send_after(Diff, self(), rotate).

close(undefined) -> ok;

close(DB) -> sqlite3:close(DB).

pmap(Fun, List) ->
	Parent = self(),
	[receive {Pid, Result} -> Result end || 
		Pid <- [spawn(fun() -> Parent ! {self(), catch Fun(E)} end)  ||
			 E <- List]].

