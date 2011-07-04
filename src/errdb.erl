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

-include("elog.hrl").

-import(extbif, [to_list/1]).

-import(lists, [concat/1,reverse/1]).

-import(errdb_misc, [b2l/1,i2l/1,l2b/1,l2a/1,number/1]).

-export([last/1,
        fetch/3,
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

-define(SERVER, {global, ?MODULE}).

-record(state, {dbtab, reqtab, journal, store, cache, dbdir}).

%ref: timer ref
-record(read_req, {id, mon, timer, from, reader}). 

-record(errdb, {key, first=0, last=0, fields = [], list=[]}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Name, Opts) ->
    gen_server2:start_link({local, Name}, ?MODULE, [Name, Opts], []).

last(Key) when is_binary(Key) -> 
    Pid = chash_pg:get_pid(?MODULE, Key),
    gen_server2:call(Pid, {last, Key}).

fetch(Key, Begin, End) when is_binary(Key) 
    and is_integer(Begin) and is_integer(End) ->
    Pid = chash_pg:get_pid(?MODULE, Key),
    gen_server2:call(Pid, {fetch, self(), Key, Begin, End}).

%data: "k=v,k1=v1,k2=v2"
insert(Key, Time, Data) when is_binary(Key) 
    and is_integer(Time) and is_binary(Data) ->
    {Fields, Values} = decode(Data),
    Pid = chash_pg:get_pid(?MODULE, Key),
    gen_server2:cast(Pid, {insert, Key, Time, {Fields, Values}}).

%Data: k=v,k1=v1,k2=v2
%return: {["k","k1","k2"], [v,v1,v2]}
decode(Data) when is_binary(Data) ->
    Tokens = binary:split(Data, [<<",">>, <<"=">>], [global]),
    decode(Tokens, []).

decode([], Acc) ->
    Sorted = 
    lists:sort(fun({Name1, _}, {Name2, _}) -> 
        Name1 =< Name2
    end, Acc),
    lists:unzip(Sorted);

decode([Name, Value|T], Acc) ->
    Tup = {b2l(Name), number(Value)},
    decode(T, [Tup|Acc]).

delete(Key) when is_binary(Key) ->
    Pid = chash_pg:get_pid(?MODULE, Key),
    gen_server2:cast(Pid, {delete, Key}).

encode({Fields, Values}) ->
    TupList = lists:zip(Fields, Values),
    Tokens = [concat([Name, "=", to_list(Val)]) || {Name, Val} <- TupList],
    string:join(Tokens, ",").

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Name, Opts]) ->
    process_flag(trap_exit, true),
    {value, Id} = dataset:get_value(id, Opts),
    {value, Dir} = dataset:get_value(dir, Opts),
    %start store process
    {ok, Store} = errdb_store:start_link(errdb_store:name(Id), Dir),

    %start journal process
    JournalOpts = proplists:get_value(journal, Opts),
    {ok, Journal} = errdb_journal:start_link(errdb_journal:name(Id), [{id, Id} | JournalOpts]),

    DbTab = ets:new(dbtab(Id), [set, protected, 
        named_table, {keypos, 2}]),
    ReqTab = ets:new(reqtab(Id), [set, protected, 
        named_table, {keypos, 2}]),

    chash_pg:create(errdb),
    chash_pg:join(errdb, self()),

    CacheSize = proplists:get_value(cache, Opts),
    io:format("~n~p is started.~n ", [Name]),

    {ok, #state{dbtab = DbTab, reqtab = ReqTab, dbdir = Dir,
        store = Store, journal = Journal,  cache = CacheSize}}.


%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({last, Key}, _From, #state{dbtab = DbTab} = State) ->
    Reply = 
    case ets:lookup(DbTab, Key) of
    [#errdb{fields=Fields, list=[Last|_]}] -> 
        {ok, Fields, Last};
    [] -> 
        {error, notfound}
    end,
    {reply, Reply, State};

handle_call({fetch, Pid, Key, Begin, End}, From, #state{dbtab = DbTab} = State) ->
    case ets:lookup(DbTab, Key) of
    [#errdb{first=First, last=Last, fields=Fields, list=List}] -> 
        case (Begin >= First) and (End =< Last) of
        true ->
            Reply = {ok, Fields, filter(Begin, End, List)},
            {reply, Reply, State};
        false -> 
            fetch_from_store({Pid, Key, Begin, End}, {Fields, List}, From, State),
            {noreply, State}
        end;
    [] -> 
        {reply, {error, notfound}, State}
    end;


handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {reply, {error, bagreq}, State}.

priorities_call({last, _}, _From, _State) ->
    10;
priorities_call({fetch,_,_,_,_}, _From, _State) ->
    10;
priorities_call(_, _From, _State) ->
    0.

check_time(Last, Time) ->
    Time > Last.
check_fields(OldFields, Fields) ->
    OldFields == Fields.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({insert, Key, Time, {Fields, Values}}, #state{dbtab = DbTab, 
    journal = Journal, store = Store, cache = CacheSize} = State) ->
    Result =
    case ets:lookup(DbTab, Key) of
    [#errdb{last=Last,fields=OldFields,list=List} = OldRecord] -> 
        case [check_time(Last, Time), check_fields(OldFields, Fields)] of
        [true, true] ->
            case length(List) >= CacheSize of
            true ->
                errdb_store:write(Store, Key, Fields, reverse(List)),
                {ok, OldRecord#errdb{first = Time, last = Time, list = [{Time, Values}]}};
            false ->
                {ok, OldRecord#errdb{last = Time, list = [{Time, Values}|List]}}
            end;
        [false, _] ->
            ?WARNING("badtime: time=~p =< last=~p ", [Time, Last]),
            {error, badtime};
        [_, false] ->
            ?WARNING("badfield: fields=~p, oldfields=~p ", [Fields, OldFields]),
            {error, badfield}
        end;
    [] ->
        {ok, #errdb{key=Key, first=Time, last=Time, fields=Fields, list=[{Time, Values}]}}
    end,
    case Result of
    {ok, NewRecord} ->
        ets:insert(DbTab, NewRecord),
        errdb_journal:write(Journal, Key, Time, encode({Fields, Values}));
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

fetch_from_store({Pid, Key, Begin, End}, {MemFields, MemList}, From, 
    #state{dbdir = Dir, reqtab = ReqTab}) ->
    Parent = self(),
    ReqId = make_ref(),
    MonRef = erlang:monitor(process, Pid),
    Timer = erlang:send_after(4000, self, {read_timeout, ReqId, From}),
    Reader = 
    spawn_link(fun() -> 
        Reply = 
        case errdb_store:read(Dir, Key) of
        {ok, Fields, Records} ->
            {ok, Fields, filter(Begin, End, MemList ++ Records)};
        {ok, []} ->
            {ok, MemFields, filter(Begin, End, MemList)};
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
    ets:insert(ReqTab, Req).

filter(Begin, End, List) ->
    Match = [{Time, Data} || {Time, Data} <- List, Time >= Begin, Time =< End],
    lists:sort(fun({T1,_}, {T2,_}) -> 
        T1 =< T2
    end, Match).

dbtab(Id) ->
    l2a("errdb_" ++ i2l(Id)).

reqtab(Id) ->
    l2a("read_req_" ++ i2l(Id)).

cancel_timer(undefined) ->
    ok;
cancel_timer(Ref) ->
    (catch erlang:cancel_timer(Ref)).

