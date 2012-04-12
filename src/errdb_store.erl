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

-export([name/1,
        start_link/2,
        read/3, %/FIXME: later
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

name(Id) ->
    l2a("errdb_store_" ++ i2l(Id)).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Name, Dir) ->
    gen_server2:start_link({local, Name}, ?MODULE, [Name, Dir], [{spawn_opt, [{min_heap_size, 204800}]}]).

read(DbDir, Key, _) ->
    FileName = filename(DbDir, Key),
    case file:read_file(FileName) of
    {ok, Binary} ->
        case decode(Binary) of
        {ok, #head{fields=Fields}=Head, DataItems} ->
			Records = [{Time, lists:zip(Fields, Values)} 
						|| {Time, Values} <- DataItems],
            ?INFO("~p ~n~p", [Head, Records]),
            {ok, Records};
        {error, Reason} ->
            {error, Reason}
        end;
    {error, enoent} ->
        {ok, []}; %no file and data.
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
    io:format("~n~p is started.~n", [Name]),
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
handle_cast({write, Key, [{_, Metrics}|_] = Records}, #state{dbdir = Dir} = State) ->
	Fields = [F || {F, _V} <- Metrics],
    FileName = filename(Dir, Key),
    filelib:ensure_dir(FileName),
    {ok, File} = file:open(FileName, [read, write, binary, raw, {read_ahead, 1024}]), 
    case file:read(File, 1024) of
    {ok, <<"RRDB0001", _/binary>> = Bin} ->
        Head = decode(head, Bin),
        #head{lastup=LastUp,lastrow=LastRow,fields=OldFields} = Head,
        case check_fields(OldFields, Fields) of
        true ->
            Lines = lines(Records),
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
        false ->
            ?ERROR("fields not match: ~p, ~p", [OldFields, Fields])
        end;
    {ok, ErrBin} ->
        ?ERROR("error file: ~p", [ErrBin]);
    eof -> %new file
        {LastUp, _} = lists:last(Records),
        LastRow = length(Records) - 1,
        Head = encode(head, Fields),
        file:write(File, Head),
        Lines = lines(Records),
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
    Path = binary:replace(list_to_binary(Key), 
		[<<",">>, <<":">>], <<"/">>, [global]),
    concat([Dir, b2l(Path), ".rrdb"]).

check_fields(OldFields, Fields) ->
    OldFields == Fields.

lines(Records) ->
    [line(Record) || Record <- Records].

%FIXME: CRITICAL BUG
line({Time, Values}) ->
    Line = [<<V/float>> || {_,V} <- Values],
    list_to_binary([<<Time:32>>, <<":">>, Line]).

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

