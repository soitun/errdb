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

-include("elog.hrl").

-import(lists, [concat/1]).

-import(extbif, [zeropad/1]).

-import(errdb, [i2b/1,b2l/1,l2b/1,b2i/1]).

-behavior(gen_server).

-export([start_link/2,
        read/2,
        write/3,
        delete/2]).

-export([init/1, 
        handle_call/3, 
        priorities_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {dbdir}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Name, Dir) ->
    gen_server2:start_link({local, Name}, ?MODULE, [Name, Dir], []).

read(DbDir, Key) ->
    FileName = filename(DbDir, Key),
    case file:read_file(FileName) of
    {ok, Data} ->
        [_|Rows] = binary:split(Data, <<"\n">>, [global]),
        List = 
        [begin 
            [Time, Value] = binary:split(Row, <<":">>, [global]),
            {b2i(Time), Value}
        end || Row <- Rows, Row =/= <<>>],
        {ok, List};
    {error, Error} -> 
        {error, Error}
    end.
    
write(Pid, Key, Records) ->
    gen_server2:cast(Pid, {write, Key, Records}).

delete(Pid, Key) ->
    gen_server2:cast(Pid, {delete, Key}).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Name, Dir]) ->
    ?INFO("~p is started.", [Name]),
    {ok, #state{dbdir = Dir}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(dbdir, _From, #state{dbdir = Dir} = State) ->
    {reply, Dir, State};

handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {reply, {error, badreq}, State}.

priorities_call(dbdir, _From, _State) ->
    10;
priorities_call(_, _From, _State) ->
    0.
%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({write, Key, Records}, #state{dbdir = Dir} = State) ->
    FileName = filename(Dir, Key),
    filelib:ensure_dir(FileName),
    {ok, File} = file:open(FileName, [read, write, append, raw, {read_ahead, 1024}]), 
    case file:read_line(File) of
    {ok, "#time:" ++ Head} ->
        Head1 = string:strip(Head, right, $\n),
        Fields = fields(l2b(Head1)),
        Lines = lines(Fields, Records),
        file:write(File, Lines);
    eof -> %new file
        Fields = fields(Records),
        file:write(File, head(Fields)),
        Lines = lines(Fields, Records),
        file:write(File, Lines);
    {error, Reason} -> 
        ?ERROR("failed to open '~p': ~p", [FileName, Reason])
    end,
    file:close(File),
    {noreply, State};

handle_cast({delete, Key}, #state{dbdir = Dir} = State) ->
    file:del_dir(filename(Dir, Key)),
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
handle_info(Info, State) ->
    ?ERROR("badinfo: ~p", [Info]),
    {noreply, State}.

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

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
filename(Dir, Key) ->
    Path = binary:replace(Key, <<",">>, <<"/">>, [global]),
    concat([Dir, b2l(Path)]).

fields([{_, Data}|_]) ->
    Tokens = binary:split(Data, <<",">>, [global]),
    Fields = 
    [begin 
        [Field|_] = binary:split(Token, <<"=">>), Field
    end || Token <- Tokens],
    lists:sort(Fields);

fields(Head) when is_binary(Head) ->
    lists:sort(binary:split(Head, <<",">>, [global])).

head(Fields) ->
    Head = string:join([b2l(Field) || Field <- Fields], ","),
    list_to_binary(["#time:", Head, "\n"]).

lines(Fields, Records) ->
    [line(Fields, Record) || Record <- Records].

line(Fields, {Time, Data}) ->
    Tokens = binary:split(Data, [<<",">>, <<"=">>], [global]),
    TupList = tuplist(Tokens, []),
    Values = [proplists:get_value(Field, TupList, <<"0">>) || Field <- Fields],
    Line = string:join([b2l(V) || V <- Values], ","),
    list_to_binary([i2b(Time), <<":">>, Line, <<"\n">>]).

tuplist([], Acc) ->
    Acc;
tuplist([Name, Val|T], Acc) ->
    tuplist(T, [{Name, Val}|Acc]).

