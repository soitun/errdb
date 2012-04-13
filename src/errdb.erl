%%%----------------------------------------------------------------------
%%% File    : errdb.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : 
%%% Created : 03 Apr. 2010
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2011, www.opengoss.com
%%%----------------------------------------------------------------------
-module(errdb).

-author('ery.lee@gmail.com').

-include_lib("elog/include/elog.hrl").

-import(extbif, [to_list/1]).

-import(lists, [concat/1,reverse/1]).

-import(proplists, [get_value/2, get_value/3]).

-export([name/1,
		info/0,
        last/1,
		last/2,
        fetch/4,
        insert/3,
        delete/1]).

-behavior(gen_server).

-export([start_link/2]).

-export([init/1, 
        handle_call/3,
        priorities_call/3,
        handle_cast/2,
        handle_info/2,
        priorities_info/2,
        terminate/2,
        code_change/3]).

-record(state, {dbtab, journal, store, cache, threshold = 1, timeout, dbdir}).

-record(errdb, {key, first=0, last=0, timer, rows=[]}). %fields = [], 

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Id, Opts) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Id, Opts],
		[{spawn_opt, [{min_heap_size, 4096}]}]).

name(Id) ->
	list_to_atom("errdb_" ++ integer_to_list(Id)).

info() ->
    Pids = chash_pg:get_pids(errdb),
    [gen_server2:call(Pid, info) || Pid <- Pids].

last(Key) when is_list(Key) ->
    Pid = chash_pg:get_pid(?MODULE, Key),
    gen_server2:call(Pid, {last, Key}).

last(Key, Fields) when is_list(Key)
	and is_list(Fields) ->
	Pid = chash_pg:get_pid(?MODULE, Key),
    gen_server2:call(Pid, {last, Key, Fields}).

fetch(Key, Fields, Begin, End) when
	is_list(Key) and is_list(Fields) 
	and is_integer(Begin) and is_integer(End) ->
	Pid = chash_pg:get_pid(?MODULE, Key),
    case gen_server2:call(Pid, {fetch, Key, Fields}) of
	{ok, DataInMem, DbDir} -> 
		case errdb_store:read(DbDir, Key, Fields) of
		{ok, DataInFile} ->
			Filter = fun(L) -> filter(Begin, End, L) end,
			YData = [{Field, Filter(Values)} || {Field, Values} 
						<- merge(Fields, DataInFile, DataInMem)],
			XData = errdb_lib:transform(YData),
			Values = fun(L) -> [get_value(F, L) || F <- Fields] end,
			{ok, lists:sort([{Time, Values(Row)} || {Time, Row} <- XData])};
		{error, Reason} ->
			{error, Reason}
		end;
	{error, Reason1} ->
		{error, Reason1}
	end.

merge(Fields, Data1, Data2) ->
	Values1 = fun(F) -> get_value(F, Data1, []) end,
	Values2 = fun(F) -> get_value(F, Data2, []) end,
	[{F, Values1(F) ++ Values2(F)} || F <- Fields].

%metrics: [{k, v}, {k, v}...]
insert(Key, Time, Metrics) when is_list(Key) 
	and is_integer(Time) and is_list(Metrics) ->
    gen_server2:cast(chash_pg:get_pid(?MODULE, Key), 
		{insert, Key, Time, Metrics}).

delete(Key) when is_list(Key) ->
    Pid = chash_pg:get_pid(?MODULE, Key),
    gen_server2:cast(Pid, {delete, Key}).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Id, Opts]) ->
    process_flag(trap_exit, true),
    {value, Dir} = dataset:get_value(dir, Opts),
	{value, VNodes} = dataset:get_value(vnodes, Opts, 40),
	Timeout = get_value(timeout, Opts, 48)*3600*1000,
    %start store process
    {ok, Store} = errdb_store:start_link(errdb_store:name(Id), Dir),

    %start journal process
    JournalOpts = proplists:get_value(journal, Opts),
    {ok, Journal} = errdb_journal:start_link(errdb_journal:name(Id), [{id, Id} | JournalOpts]),

    DbTab = ets:new(dbtab(Id), [set, protected, 
        named_table, {keypos, 2}]),

    chash_pg:create(errdb),
    chash_pg:join(errdb, self(), VNodes),

    CacheSize = proplists:get_value(cache, Opts),
    io:format("~n~p is started.~n ", [name(Id)]),

    erlang:send_after(1000, self(), cron),

    {ok, #state{dbtab = DbTab, dbdir = Dir,
		store = Store, journal = Journal,
		cache = CacheSize,
		timeout = Timeout}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(info, _From, #state{store=Store, journal=Journal} = State) ->
    Reply = [errdb_misc:pinfo(self()), 
            errdb_misc:pinfo(Store),
            errdb_misc:pinfo(Journal)],
    {reply, Reply, State};
    
handle_call({last, Key}, _From, #state{dbtab = DbTab} = State) ->
    Reply = 
    case ets:lookup(DbTab, Key) of
    [#errdb{rows=[{Time, Metrics}|_]}] -> 
		{Fields, Values} = lists:unzip(Metrics),	
        {ok, Time, Fields, Values};
    [] -> 
        {error, notfound}
    end,
    {reply, Reply, State};

handle_call({last, Key, Fields}, _From, #state{dbtab = DbTab} = State) ->
    Reply = 
    case ets:lookup(DbTab, Key) of
    [#errdb{rows=[{Time, Metrics}|_]}] -> 
        {ok, Time, Fields, values(Fields, Metrics)};
    [] ->
        {error, notfound}
    end,
    {reply, Reply, State};

handle_call({fetch, Key, Fields}, _From, 
	#state{dbdir= DbDir, dbtab = DbTab} = State) ->
    case ets:lookup(DbTab, Key) of
    [#errdb{rows=Rows}] -> 
		Data = lists:map(fun(Field) -> 
			Value = fun(Row) -> get_value(Field, Row) end,
			Values = [{T, Value(Row)} || {T, Row} <- Rows],
			{Field, lists:sort(Values)}
		end, Fields),
		{reply, {ok, Data, DbDir}, State};
    [] -> 
        {reply, {error, notfound}, State}
    end;

handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.

priorities_call(info, _From, _State) ->
    10;
priorities_call({last, _}, _From, _State) ->
    10;
priorities_call({last, _, _}, _From, _State) ->
    10;
priorities_call({fetch, _, _, _}, _From, _State) ->
    10;
priorities_call(_, _From, _State) ->
    0.

check_time(Last, Time) ->
    Time > Last.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({insert, Key, Time, Metrics}, #state{dbtab = DbTab, 
    journal = Journal, store = Store, cache = CacheSize,
	timeout = Timeout, threshold = Threshold} = State) ->
    Result =
    case ets:lookup(DbTab, Key) of
    [#errdb{last=Last, rows=List, timer=Timer} = OldRecord] ->
        case check_time(Last, Time) of
        true ->
            case length(List) >= (CacheSize+Threshold) of
            true ->
				NewTimer = reset_timer(Timer, Key, Timeout),
                errdb_store:write(Store, Key, reverse(List)),
                {ok, OldRecord#errdb{first = Time, last = Time, 
					timer = NewTimer, rows = [{Time, Metrics}]}};
            false ->
                {ok, OldRecord#errdb{last = Time, rows = [{Time, Metrics}|List]}}
            end;
        false ->
            ?WARNING("key: ~p, badtime: time=~p =< last=~p", [Key, Time, Last]),
            {error, badtime}
        end;
    [] ->
        {ok, #errdb{key=Key, first=Time, last=Time, rows=[{Time, Metrics}]}}
    end,
    case Result of
    {ok, NewRecord} ->
        ets:insert(DbTab, NewRecord),
        errdb_journal:write(Journal, Key, Time, Metrics);
    {error, _Reason} ->
        ignore %here
    end,
    {noreply, State};

handle_cast({delete, Key}, #state{store = Store, dbtab = DbTab} = State) ->
    errdb_store:delete(Store, Key),
    ets:delete(DbTab, Key),
    {noreply, State};

handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.

%TODO: DEPRECATED CODE???
handle_info({'EXIT', _Pid, normal}, State) ->
    %Reader pid is normaly down 
    {noreply, State};

handle_info({timeout, Key}, State) ->
	handle_cast({delete, Key}, State);
    
handle_info(cron, #state{cache = CacheSize} = State) ->
    Threshold = random:uniform(CacheSize),
    erlang:send_after(1000, self(), cron),
    {noreply, State#state{threshold = Threshold}};

handle_info(Info, State) ->
    ?ERROR("badinfo: ~p", [Info]),
    {noreply, State}.

priorities_info(cron, _State) ->
    11;
priorities_info({read_rep,_,_}, _State) ->
    10;
priorities_info({read_timeout,_,_}, _State) ->
    10;
priorities_info(_, _) ->
    1.
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

values(Fields, Metrics) ->
	[get_value(F, Metrics) || F <- Fields].

filter(Begin, End, List) ->
    [E || {Time, _} = E <- List, Time >= Begin, Time =< End].

dbtab(Id) ->
    list_to_atom("errdb_" ++ integer_to_list(Id)).

reset_timer(Ref, Key, Timeout) ->
	cancel_timer(Ref),
	erlang:send_after(Timeout, self(), {timeout, Key}).

cancel_timer(undefined) ->
    ok;

cancel_timer(Ref) ->
    (catch erlang:cancel_timer(Ref)).

