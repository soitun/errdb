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

-import(lists, [concat/1, reverse/1]).

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

-record(head, {lastup, lastrow, dscnt, dssize, fields}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Name, Dir) ->
    gen_server2:start_link({local, Name}, ?MODULE, [Name, Dir], []).

read(DbDir, Key) ->
    FileName = filename(DbDir, Key),
    case file:read_file(FileName) of
    {ok, Binary} ->
        case decode(Binary) of
        {ok, #head{fields = Fields} = Head, Records} ->
            ?INFO("head: ~p", [Head]),
            {ok, Fields, Records};
        {error, Reason} ->
            {error, Reason}
        end;
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
handle_cast({write, Key, Records}, #state{dbdir = Dir} = State) ->
    FileName = filename(Dir, Key),
    filelib:ensure_dir(FileName),
    {ok, File} = file:open(FileName, [read, write, binary, raw, {read_ahead, 1024}]), 
    case file:read(File, 1024) of
    {ok, <<"RRDB0001", _/binary>> = Bin} ->
        Head = decode(head, Bin),
        #head{lastup=LastUp,lastrow=LastRow,fields=Fields} = Head,
        Lines = lines(Fields, Records),
        {LastUp1, LastRow1, Writes} = 
        lists:foldl(fun(Line, {_Up, Row, Acc}) -> 
            <<Time:32,_/binary>> = Line,
            NextRow =
            if
            Row >= 599 -> 0;
            true -> Row + 1
            end,
            Pos = 1024 + size(Line) * NextRow,
            {Time, NextRow, [{Pos, Line}|Acc]}
        end, {LastUp, LastRow, []}, Lines),
        Writes1 = [{8, <<LastUp1:32, LastRow1:32>>}|Writes],
        file:pwrite(File, Writes1);
    eof -> %new file
        Fields = fields(Records),
        {LastUp,_} = lists:last(Records),
        LastRow = length(Records) - 1,
        Head = encode(head, Fields),
        file:write(File, Head),
        Lines = lines(Fields, Records),
        Writes = [{8, <<LastUp:32, LastRow:32>>},{1024, l2b(Lines)}],
        file:pwrite(File, Writes);
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
        [Field|_] = binary:split(Token, <<"=">>), b2l(Field)
    end || Token <- Tokens],
    lists:sort(Fields);

fields(Head) when is_binary(Head) ->
    lists:sort(binary:split(Head, <<",">>, [global])).

lines(Fields, Records) ->
    [line(Fields, Record) || Record <- Records].

line(Fields, {Time, Data}) ->
    Tokens = binary:split(Data, [<<",">>, <<"=">>], [global]),
    TupList = tuplist(Tokens, []),
    Values = [proplists:get_value(l2b(Field), TupList, 0.0) || Field <- Fields],
    Line = [<<V/float>> || V <- Values],
    list_to_binary([<<Time:32>>, <<":">>, Line]).

tuplist([], Acc) ->
    Acc;
tuplist([Name, ValBin|T], Acc) ->
    Val = binary_to_number(ValBin),
    tuplist(T, [{Name, Val}|Acc]).

binary_to_number(Bin) ->
    N = binary_to_list(Bin),
    case string:to_float(N) of
        {error,no_float} -> list_to_integer(N);
        {F,_Rest} -> F
    end.

decode(Bin) ->
    <<HeadBin:1024/binary, BodyBin/binary>> = Bin,
    #head{fields = Fields} = Head = decode(head, HeadBin),
    Records = decode(body, Fields, BodyBin),
    {ok, Head, Records}.

decode(head, Bin) ->
    <<"RRDB0001", LastUp:32, LastRow:32, DsCnt:32, DsSize:32, DsBin/binary>> = Bin,
    <<DsData:DsSize/binary, _/binary>> = DsBin,
    Fields = [b2l(B) || B <- binary:split(DsData, <<",">>, [global])],
    #head{lastup=LastUp, lastrow=LastRow, dscnt=DsCnt, dssize=DsSize,fields=Fields};

decode(value, Bin) ->
    decode(value, Bin, []).

decode(body, Fields, Bin) ->
    Len = length(Fields) * 8,
    decode(line, Bin, Len);

decode(line, Bin, Len) ->
    decode(line, Bin, Len, []);

decode(value, <<>>, Acc) ->
    reverse(Acc);

decode(value, <<V/float, Bin/binary>>, Acc) ->
    decode(value, Bin, [V | Acc]).

decode(line, <<>>, _Len, Acc) ->
    reverse(Acc);

decode(line, <<Time:32, ":", Bin/binary>>, Len, Acc) ->
    <<ValBin:Len/binary, Left/binary>> = Bin, 
    Record = {Time, decode(value, ValBin)},
    decode(line, Left, Len, [Record|Acc]).

encode(head, Fields) ->
    DsData = l2b(string:join(Fields, ",")),
    DsCnt = length(Fields),
    DsSize = size(DsData),
    <<"RRDB0001", 0:32, 0:32, DsCnt:32, DsSize:32, DsData/binary>>;

encode(line, {Time, Values}) ->
    ValBin = l2b([<<V/float>> || V <- Values]),
    <<Time:32, ":", ValBin/binary>>.

