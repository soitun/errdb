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

-import(lists, [reverse/1]).

-include("elog.hrl").

-export([last/1,
        fetch/3,
        insert/3,
        delete/1]).

-behavior(gen_server).

-export([start_link/1]).

-export([init/1, 
        handle_call/3, 
        priorities_call/3,
        handle_cast/2,
        handle_info/2,
        priorities_info/2,
        terminate/2,
        code_change/3]).

-record(state, {cache = 12}).

%ref: timer ref
-record(read_req, {id, mon, timer, from, reader}). 

-record(errdb, {key, first=0, last=0, list=[]}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Opts) ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

last(Key) when is_binary(Key) -> 
    case ets:lookup(errdb, Key) of
    [#errdb{list = [Last|_]}] -> {ok, Last};
    [] -> {error, notfound}
    end.

fetch(Key, Begin, End) when is_binary(Key) 
    and is_integer(Begin) and is_integer(End) ->
    case ets:lookup(errdb, Key) of
    [#errdb{first = First, last = Last, list = List}] -> 
        case (Begin >= First) and (End =< Last) of
        true ->
            {ok, filter(Begin, End, List)};
        false -> %TODO: should read from file.
            gen_server2:call(?MODULE, {fetch, self(), Key, Begin, End})
        end;
    [] -> 
        {ok, []}
    end.

filter(Begin, End, List) ->
    [{Time, Data} || {Time, Data} <- List, Time >= Begin, Time =< End].

insert(Key, Time, Value) when is_binary(Key) 
    and is_integer(Time) and is_binary(Value) ->
    gen_server2:cast(?MODULE, {insert, Key, Time, Value}).

delete(Key) when is_binary(Key) ->
    gen_server2:cast(?MODULE, {delete, Key}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Opts]) ->
    process_flag(trap_exit, true),
    {value, Size} = dataset:get_value(cache, Opts, 12),
    ets:new(errdb, [set, protected, named_table, {keypos, 2}]),
    ets:new(read_req, [set, protected, named_table, {keypos, 2}]),
    ?INFO("errdb is started, cache: ~p", [Size]),
    {ok, #state{cache = Size}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({fetch, Pid, Key, Begin, End}, From, State) ->
    ReqId = make_ref(),
    MonRef = erlang:monitor(process, Pid),
    Timer = erlang:send_after(4000, self, {read_timeout, ReqId, From}),
    Reader = 
    spawn_link(fun() -> 
        Reply = 
        case errdb_store:read(Key) of
        {ok, Records} ->
            {ok, filter(Begin, End, Records)};
        {error, Reason} ->
            {error, Reason}
        end,
        ?MODULE ! {read_rep, ReqId, Reply}  
    end),
    Req = #read_req{id = ReqId, 
        mon = MonRef, 
        timer = Timer,
        from = From,
        reader = Reader},
    ets:insert(read_req, Req),
    {noreply, State};

handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {reply, {error, bagreq}, State}.

priorities_call({fetch,_,_,_,_}, _From, _State) ->
    10;
priorities_call(_, _From, _State) ->
    0.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({insert, Key, Time, Value}, #state{cache = CacheSize} = State) ->
    NewRecord =
    case ets:lookup(errdb, Key) of
    [#errdb{list = List} = OldRecord] -> 
        case length(List) >= CacheSize of
        true ->
            errdb_store:write(Key, reverse(List)),
            OldRecord#errdb{first = Time, last = Time, list = [{Time, Value}]};
        false ->
            OldRecord#errdb{last = Time, list = [{Time, Value}|List]}
        end;
    [] ->
        #errdb{key = Key, first = Time, last = Time, list = [{Time, Value}]}
    end,
    ets:insert(errdb, NewRecord),
    errdb_journal:write(Key, Time, Value),
    {noreply, State};

handle_cast({delete, Key}, State) ->
    errdb_store:delete(Key),
    ets:delete(errdb, Key),
    {noreply, State};

handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({read_rep, ReqId, Reply}, State) ->
    case ets:lookup(read_req, ReqId) of
    [#read_req{from = From, timer = Timer, mon = MonRef}] ->
        cancel_timer(Timer),
        erlang:demonitor(MonRef),
        gen_server2:reply(From, Reply),
        ets:delete(read_req, ReqId);
    [] ->
        ?ERROR("delay read_req: ~p, ~p", [ReqId, Reply])
    end,
    {noreply, State};

handle_info({read_timeout, ReqId, From}, State) ->
    case ets:lookup(read_req, ReqId) of
    [#read_req{from = From, mon = MonRef}] ->
        erlang:demonitor(MonRef),
        gen_server2:reply(From, {error, timeout}),
        ets:delete(read_req, ReqId);
    [] ->
        ?ERROR("unexepected read_timeout: ~p", [ReqId])
    end,
    {noreply, State};

handle_info({'DOWN', MonRef, process, _Pid, _Reason}, State) ->
    Pat = #read_req{id = '$1', timer = '$2', mon = MonRef, _ = '_'},
    Match = ets:match(read_req, Pat),
    lists:foreach(fun([ReqId, Timer]) -> 
        cancel_timer(Timer),
        ets:delete(read_req, ReqId)
	end, Match),
    {noreply, State};

handle_info({'EXIT', _Pid, normal}, State) ->
    %Reader pid is normaly down 
    {noreply, State};

handle_info({'EXIT', Pid, Reason}, State) ->
    ?ERROR("~p", [Reason]),
    Pat = #read_req{id = '$1', timer = '$2', mon = '$3', from = '$4', reader = Pid},
    Match = ets:match(read_req, Pat),
    lists:foreach(fun([ReqId, Timer, MonRef, From]) -> 
        cancel_timer(Timer),
        erlang:demonitor(MonRef),
        gen_server2:reply(From, {error, Reason}),
        ets:delete(read_req, ReqId)
	end, Match),
    {noreply, State};
    
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

cancel_timer(undefined) ->
    ok;
cancel_timer(Ref) ->
    (catch erlang:cancel_timer(Ref)).
