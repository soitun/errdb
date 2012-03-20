%%%----------------------------------------------------------------------
%%% File    : errdb_store.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : File Storage 
%%% Created : 03 Apr. 2010
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2011, www.opengoss.com
%%%----------------------------------------------------------------------
-module(errdb_store).

-author('ery.lee@gmail.com').

-include_lib("elog/include/elog.hrl").

-import(extbif, [zeropad/1]).

-import(lists, [concat/1, reverse/1]).

-import(errdb_misc, [b2l/1, i2l/1, l2a/1, l2b/1]).

-behavior(gen_server).

-export([start_link/2,
		name/1,
        insert/2]).

-export([init/1, 
        handle_call/3, 
        priorities_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {db, dir}).

-record(head, {lastup, lastrow, dscnt, dssize, fields}).

-define(SCHEMA, "CREATE TABLE metrics (id INTEGER PRIMARY KEY, node TEXT, metric INTEGER, timestamp INTEGER, value REAL, CHECK ('am'='am'));").

-define(INDEX, "CREATE INDEX node_time_idx on metrics(node, timestamp);").

name(Id) ->
    l2a("errdb_store_" ++ i2l(Id)).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Id, Dir) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Id, Dir], [{spawn_opt, [{min_heap_size, 204800}]}]).

insert(Pid, Records) ->
	gen_server2:cast(Pid, {insert, Records}).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Id, Dir]) ->
	File = lists:concat([Dir, "/", i2l(Id), ".db"]),
	Db = list_to_atom(lists:concat(["errdb", i2l(Id)])),
	{ok, _} = sqlite3:open(Db, [{file, File}]),
	Tabs = [{Tab, sqlite3:table_info(Db, Tab)}
			|| Tab <- sqlite3:list_tables(Db)],
	case proplists:get_value(metrics, Tabs) of
	undefined ->
		%sqlite3:sql_exec(Db, "pragma synchronous=off;"),
		sqlite3:sql_exec(Db, ?SCHEMA),
		sqlite3:sql_exec(Db, ?INDEX);
	_Schema ->
		ok
	end,
    {ok, #state{db = Db}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {reply, {error, badreq}, State}.

priorities_call(_, _From, _State) ->
    0.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({insert, Records}, #state{db = Db} = State) ->
	sqlite3:write_many(Db, metrics, Records),
	{noreply, State};

handle_cast(Msg, State) ->
    {stop, {error, {badmsg, Msg}}, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    {stop, {error, {badinfo, Info}}, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

