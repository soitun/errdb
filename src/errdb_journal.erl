%%%----------------------------------------------------------------------
%%% File    : errdb_journal.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Hourly Log.
%%% Created : 03 Apr. 2010
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2011, www.opengoss.com
%%%----------------------------------------------------------------------
-module(errdb_journal).

-author('ery.lee@gmail.com').

-import(errdb_misc, [i2b/1]).

-import(extbif, [timestamp/0, zeropad/1]).

-import(errdb_misc, [i2l/1, l2a/1]).

-include("elog.hrl").

-behavior(gen_server).

-export([name/1,
        start_link/2, 
        info/1,
        write/4]).

-export([init/1, 
        handle_call/3, 
        priorities_call/3,
        handle_cast/2,
        handle_info/2,
        priorities_info/2,
        terminate/2,
        code_change/3]).

-record(state, {id, logdir, logfile, thishour, buffer_size = 100, queue = []}).

name(Id) ->
    l2a("errdb_journal_" ++ i2l(Id)).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Name, Opts) ->
    gen_server2:start_link({local, Name}, ?MODULE, [Name, Opts], [{spawn_opt, [{min_heap_size, 256000}]}]).

info(Pid) ->
    gen_server2:call(Pid, info).

write(Pid, Key, Time, Value) ->
    gen_server2:cast(Pid, {write, Key, Time, Value}).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Name, Opts]) ->
    Id = proplists:get_value(id, Opts),
    Dir = proplists:get_value(dir, Opts),
    BufferSize = proplists:get_value(buffer, Opts, 100),
    State = #state{id = Id, logdir = Dir, buffer_size = BufferSize},
    {noreply, NewState} = handle_info(journal_rotation, State),
    erlang:send_after(3000, self(), flush_queue),
    io:format("~n~p is started.~n", [Name]),
    {ok, NewState}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(info, _From, #state{thishour = H} = State) ->
    Info = [{hour, H} | get()],
    {reply, {ok, Info}, State};
    
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {reply, {error, badreq}, State}.

priorities_call(info, _From, _State) ->
    3.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({write, Key, Time, Value}, #state{logfile = LogFile, 
    buffer_size = MaxSize, queue = Q} = State) ->
    case length(Q) >= MaxSize of
    true ->
        incr(commit),
        flush_to_disk(LogFile, [{Key, Time, Value}|Q]),
        {noreply, State#state{queue = []}};
    false ->
        NewQ = [{Key, Time, Value} | Q],
        {noreply, State#state{queue = NewQ}}
    end;
    
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(journal_rotation, #state{id = Id, logdir = Dir, logfile = File, queue = Q} = State) ->
    flush_queue(File, Q),
    close_file(File),
    Now = timestamp(),
    {Hour,_,_} = time(),
    FilePath = lists:concat([Dir, zeropad(Hour), "/", integer_to_list(Id), ".journal"]),
    filelib:ensure_dir(FilePath),
    {ok, NewFile} = file:open(FilePath, [write]),
    NextHour = ((Now div 3600) + 1) * 3600,
    erlang:send_after((NextHour + 60 - Now) * 1000, self(), journal_rotation),
    {noreply, State#state{logfile = NewFile, thishour = Hour, queue = []}};

handle_info(flush_queue, #state{logfile = File, queue = Q} = State) ->
    flush_queue(File, Q),
    erlang:send_after(3000, self(), flush_queue),
    {noreply, State#state{queue = []}};

handle_info(Info, State) ->
    ?ERROR("badinfo: ~p", [Info]),
    {noreply, State}.

priorities_info(journal_rotation, _State) ->
    10;
priorities_info(flush_queue, _State) ->
    5.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, #state{logfile = LogFile, queue = Q}) ->
    flush_queue(LogFile, Q),
    close_file(LogFile),
    ok.
%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
incr(Key) ->
    case get(Key) of
    undefined -> put(Key, 1);
    V -> put(Key, V+1)
    end.

close_file(undefined) ->
    ok;

close_file(File) ->
    file:close(File).

flush_queue(undefined, _Q) ->
    ok;
flush_queue(_File, Q) when length(Q) == 0 ->
    ok;
flush_queue(File, Q) ->
    flush_to_disk(File, Q).

flush_to_disk(LogFile, Q) ->
    Lines = [line(K, Ts, V) || {K, Ts, V} <- lists:reverse(Q)],
    file:write(LogFile, Lines).

line(Key, Time, Value) ->
    list_to_binary([Key, <<"@">>, i2b(Time), <<":">>, Value, <<"\n">>]).

