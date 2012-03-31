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

-import(lists, [concat/1, reverse/1]).

-import(proplists, [get_value/2, get_value/3]).

-export([name/1,
		info/0,
		last/1,
		last/2,
        fetch/4,
        insert/3]).

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

%ref: timer ref
-record(read_req, {id, mon, timer, from, reader}).

%records: [{Timestamp, Metrics},...]
-record(errdb_last, {object, time, metrics=[]}).

%cache objects
-record(errdb, {object, first, last, timeout_timer, records=[]}).

-record(state, {name, dbtab, lasttab, reqtab, store, 
				buffer=[], buffer_size, 
				commit_size, commit_timer}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Id, Env) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Id, Env], 
		[{spawn_opt, [{min_heap_size, 4096}]}]).

name(Id) ->
	list_to_atom("errdb_" ++ integer_to_list(Id)).

info() ->
    [gen_server2:call(Pid, info) || 
		Pid <- chash_pg:get_pids(?MODULE)].

last(Object) when is_list(Object) ->
    gen_server2:call(chash_pg:get_pid(?MODULE, Object),
		{last, Object}).

last(Object, Fields) when is_list(Object)
	and is_list(Fields) ->
    gen_server2:call(chash_pg:get_pid(?MODULE, Object),
		{last, Object, Fields}).

fetch(Object, Fields, Begin, End) when
	is_list(Object) and is_list(Fields) 
	and is_integer(Begin) and is_integer(End) ->
    gen_server2:call(chash_pg:get_pid(?MODULE, Object),
		{fetch, self(), Object, Fields, Begin, End}, 15000).

%metrics: [{k, v}, {k, v}...]
insert(Object, Time, Metrics) when is_list(Object) 
	and is_integer(Time) and is_list(Metrics) ->
    gen_server2:cast(chash_pg:get_pid(?MODULE, Object), 
		{insert, Object, Time, Metrics}).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Id, Env]) ->
	put(commit, 0),
	put(commit_max, 0),
	put(commit_avg, 0),
	put(commit_rows, 0),
	random:seed(now()),
    process_flag(trap_exit, true),
    {value, Dir} = dataset:get_value(store, Env),
	{value, VNodes} = dataset:get_value(vnodes, Env, 40),
    %start store process
    {ok, Store} = errdb_store:start_link(Id, Dir),

    DbTab = ets:new(dbtab(Id), [set, protected, 
        named_table, {keypos, 2}]),
    LastTab = ets:new(lasttab(Id), [set, protected, 
        named_table, {keypos, 2}]),
    ReqTab = ets:new(reqtab(Id), [set, protected, 
        named_table, {keypos, 2}]),

    chash_pg:create(?MODULE),
    chash_pg:join(?MODULE, self(), VNodes),

    Size = proplists:get_value(buffer, Env),
    ?INFO("~p is started.", [name(Id)]),
    erlang:send_after(1000+random:uniform(1000), self(), commit),
    {ok, #state{name = name(Id), store = Store, 
		reqtab = ReqTab, dbtab = DbTab, lasttab = LastTab,
		buffer_size = Size, commit_size = Size}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(info, _From, #state{name = Name} = State) ->
    {reply, {Name, get()}, State};
    
handle_call({last, Object}, _From, #state{lasttab = LastTab} = State) ->
    case ets:lookup(LastTab, Object) of
	[#errdb_last{time = Time, metrics = Metrics}] ->
		{Fields, Values} = lists:unzip(Metrics),
		{reply, {ok, Time, Fields, Values}, State};
	[] ->
		{reply, {error, notfound}, State}
	end;

	
handle_call({last, Object, Fields}, _From, #state{lasttab = LastTab} = State) ->
    case ets:lookup(LastTab, Object) of
    [#errdb_last{time = Time, metrics = Metrics}] -> 
		Values = [get_value(Field, Metrics, "NaN") || Field <- Fields],
		{reply, {ok, Time, Values}, State};
    [] -> 
		{reply, {error, notfound}, State}
    end;

handle_call({fetch, Pid, Object, Fields, Begin, End}, From, 
	#state{store = Store, dbtab = DbTab, reqtab = ReqTab} = State) ->
    case ets:lookup(DbTab, Object) of
    [#errdb{records = Records}] -> 
		Values = fun(Metrics) -> 
			[get_value(F, Metrics, "NaN") || F <- Fields]
		end,
		Reply = [{Time, Values(Metrics)} || {Time, Metrics}
			<- Records, Time >= Begin, Time =< End],
		{reply, {ok, Reply}, State};
    [] -> 
		Parent = self(),
		ReqId = make_ref(),
		MonRef = erlang:monitor(process, Pid),
		Timer = erlang:send_after(10000, self, {read_timeout, ReqId, From}),
		Reader = 
		spawn_link(fun() -> 
			Reply = 
			case errdb_store:read(Store, Object, Fields, Begin, End) of
			{ok, Records} ->
				{ok, Records};
			{error, Reason} ->
				{error, Reason}
			end,
			Parent ! {read_rep, ReqId, Reply}  
		end),
		Req = #read_req{id = ReqId, 
			mon = MonRef, 
			timer = Timer,
			from = From,
			reader = Reader},
		ets:insert(ReqTab, Req),
        {noreply, State}
    end;

handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {reply, {error, bagreq}, State}.

priorities_call(info, _From, _State) ->
    10;
priorities_call({last, _}, _From, _State) ->
    10;
priorities_call({last, _, _}, _From, _State) ->
    10;
priorities_call({fetch,_,_,_,_,_}, _From, _State) ->
    10;
priorities_call(_, _From, _State) ->
    0.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({insert, Object, Time, Metrics}, #state{lasttab = LastTab, 
    commit_size = CommitSize, buffer = Buffer} = State) ->
	LastTime = last_time(ets:lookup(LastTab, Object)),
	case LastTime < Time of
	true ->
		ets:insert(LastTab, #errdb_last{object = Object,
			time = Time, metrics = Metrics}),
		NewBuffer = [{Object, Time, Metrics} | Buffer],
		case length(NewBuffer) >= CommitSize of
		true ->
			handle_info(commit, State#state{buffer = NewBuffer});
		false ->
			{noreply, State#state{buffer = NewBuffer}}
		end;
	false ->
		?WARNING("badtime error! object: ~p, lasttime: ~p, time: ~p",
			[Object, LastTime, Time]),
		{noreply, State}
	end;

handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({read_rep, ReqId, Reply}, #state{reqtab = ReqTab} = State) ->
    case ets:lookup(ReqTab, ReqId) of
    [#read_req{from = From, timer = Timer, mon = MonRef}] ->
        cancel_timer(Timer),
        erlang:demonitor(MonRef),
        gen_server2:reply(From, Reply),
        ets:delete(ReqTab, ReqId);
    [] ->
        ?ERROR("delay read_req: ~p, ~p", [ReqId, Reply])
    end,
    {noreply, State};

handle_info({read_timeout, ReqId, From}, #state{reqtab = ReqTab} = State) ->
    case ets:lookup(ReqTab, ReqId) of
    [#read_req{from = From, mon = MonRef}] ->
        erlang:demonitor(MonRef),
        gen_server2:reply(From, {error, timeout}),
        ets:delete(ReqTab, ReqId);
    [] ->
        ?ERROR("unexepected read_timeout: ~p", [ReqId])
    end,
    {noreply, State};

handle_info({'DOWN', MonRef, process, _Pid, _Reason}, #state{reqtab = ReqTab} = State) ->
    Pat = #read_req{id = '$1', timer = '$2', mon = MonRef, _ = '_'},
    Match = ets:match(ReqTab, Pat),
    lists:foreach(fun([ReqId, Timer]) -> 
        cancel_timer(Timer),
        ets:delete(ReqTab, ReqId)
	end, Match),
    {noreply, State};

handle_info(commit, #state{store = Store, buffer = Buffer,
	buffer_size = BufferSize, commit_timer = Timer} = State) ->
	cancel_timer(Timer),
	commit(Store, Buffer),
    CommitSize = BufferSize + random:uniform(BufferSize),
	Timer1 = erlang:send_after(1000, self(), commit),
	{noreply, State#state{buffer = [], commit_size = CommitSize, commit_timer = Timer1}};

%TODO: DEPRECATED CODE???
handle_info({'EXIT', _Pid, normal}, State) ->
    %Reader pid is normaly down 
    {noreply, State};

handle_info({'EXIT', Pid, Reason}, #state{reqtab = ReqTab} = State) ->
    ?ERROR("~p", [Reason]),
    Pat = #read_req{id = '$1', timer = '$2', mon = '$3', from = '$4', reader = Pid},
    Match = ets:match(ReqTab, Pat),
    lists:foreach(fun([ReqId, Timer, MonRef, From]) -> 
        cancel_timer(Timer),
        erlang:demonitor(MonRef),
        gen_server2:reply(From, {error, Reason}),
        ets:delete(ReqTab, ReqId)
	end, Match),
    {noreply, State};

handle_info({timeout, Key}, State) ->
	handle_cast({delete, Key}, State);
    
handle_info(Info, State) ->
    ?ERROR("badinfo: ~p", [Info]),
    {noreply, State}.

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

commit(_Store, []) ->
	ignore;
commit(Store, Records) ->
	Rows =
	lists:foldl(fun({Object, Time, Metrics}, Acc) ->
		Row = fun(Metric, Value) ->
			[{object, Object}, {time, Time},
			 {metric, Metric}, {value, Value}]
		end,
		Rows = [Row(Metric, Value) || {Metric, Value} <- Metrics],
		Rows ++ Acc
	end, [], Records),
	Length = length(Rows),
	incr(commit),
	incr(commit_rows, Length),
	put(commit_avg, get(commit_rows) div get(commit)),
	case Length > get(commit_max) of
	true ->
		put(commit_max, Length);
	false ->
		ok
	end,
	errdb_store:write(Store, Rows).

dbtab(Id) ->
    list_to_atom("errdb_" ++ integer_to_list(Id)).

lasttab(Id) ->
    list_to_atom("errdb_last_" ++ integer_to_list(Id)).

reqtab(Id) ->
    list_to_atom("read_req_" ++ integer_to_list(Id)).

last_time([]) ->
	0;
last_time([#errdb_last{time = LastTime}]) ->
    LastTime.

cancel_timer(undefined) ->
    ok;
cancel_timer(Ref) ->
    (catch erlang:cancel_timer(Ref)).

incr(Key) ->
	incr(Key, 1).
incr(Key, Count) ->
    case get(Key) of
    undefined -> put(Key, Count);
    V -> put(Key, V+Count)
    end.
