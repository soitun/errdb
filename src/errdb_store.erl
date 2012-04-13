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
        read/3, 
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

-record(head, {ver, dscnt, dslist}).

%name length 24
-record(ds, {name, pos, lastrow, lastup, lastval}).

-define(HEAD_SIZE, 4096).

-define(MAX_DS_NAME_LEN, 24).

-define(MAX_DS_ROWS, 600).

-define(DS_HEAD_LEN, 45).

-define(PAGE_SIZE, (?MAX_DS_ROWS*12)).

name(Id) ->
    l2a("errdb_store_" ++ i2l(Id)).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Name, Dir) ->
    gen_server2:start_link({local, Name}, ?MODULE, [Name, Dir],
		[{spawn_opt, [{min_heap_size, 204800}]}]).

read(DbDir, Key, Fields) ->
    FileName = filename(DbDir, Key),
	case file:open(FileName, [read, binary, raw, {read_ahead, 4096}] of
	{ok, File} ->
		case read_head(File, Fields) of
		{ok, Head} -> read_data(File, Head, Fields);
		Error -> Error
		end;
	{error, enoent} ->
		{ok, []};
    {error, Error} -> 
        {error, Error}
	end.

read_head(File, Fields) ->
	case file:read(File, 4096) of
    {ok, <<"RRDB0002", _/binary>> = Data} ->
		{ok, decode(head, Data)};
	{ok, _} ->
		{error, badhead};
	eof ->
		{error, nohead};
	{error, Reason} ->
		{error, Reason}
	end.

read_data(File, Head, Fields) ->
	Reads = [ {get_ds_pos(Ds), ?PAGE_SIZE} || 
		Ds <- [get_ds(F, Head) || F <- Fields] ],
	{ok, DataList} = file:pread(File, Reads),
	lists:zip(Fields, [decode_data(Data) || Data <- DataList]).

get_ds_pos(#ds{pos = Pos}) ->
	?HEAD_SIZE + Pos * ?PAGE_SIZE.
    
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
    {stor, {error, {badreq, Req}}, State}.

priorities_call(_, _From, _State) ->
    0.
%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({write, Key, Rows}, #state{dbdir = Dir} = State) ->
    FileName = filename(Dir, Key),
    filelib:ensure_dir(FileName),
    {ok, File} = file:open(FileName, [read, write, binary, raw, {read_ahead, 4096}]), 
    case file:read(File, 4096) of
    {ok, <<"RRDB0002", _/binary>> = Bin} ->
		update_rrdb(File, decode(head, Bin), Datalogs);
    {ok, ErrBin} ->
        ?ERROR("error file: ~p", [ErrBin]);
    eof -> %new file
		create_rrdb(File, Datalogs);
    {error, Reason} -> 
        ?ERROR("failed to open '~p': ~p", [FileName, Reason])
    end,
    file:close(File),
    {noreply, State};

handle_cast({delete, Key}, #state{dbdir = Dir} = State) ->
    file:del_dir(filename(Dir, Key)),
    {noreply, State};
    
handle_cast(Msg, State) ->
    {stop, {error, {badcast, Msg}}, State}.

handle_info(Info, State) ->
    {stop, {error, {badinfo, Info}}, State}.

terminate(_Reason, _State) ->
    ok.

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

create_rrdb(File, Rows) ->
	{DsCnt, DsList, Writes} 
	list:foldl(fun({Col, Values}, {Idx, DsAcc, ValAcc}) -> 
		LastRow = length(Values),
		{LastUp, LastVal} = lists:last(Values),
		Ds = #ds{name = Col, pos = Idx, lastrow = LastRow,
				 lastup = LastUp, lastval = LastVal}, 
		Write = {4096+Idx*600*12, lines(Values),
		{Idx+1, [Ds|DsAcc], [Write|ValAcc]}
	end, {0, [], []}, row2column(Rows)),
	Head = #head{ver = <<"RRDB0002">>, dscnt = DsCnt,
				dslist = lists:reverse(DsList)},
	file:write(File, encode(Head)),
	file:pwrite(File, Writes);

update_rrdb(File, #head{dslist = DsList} = Head, Rows) ->
	Columns = row2column(Rows),
	Fields = [Name || {Name,_} <- Columns],
	OldFields = [Name || #ds{name=Name} <- DsList],
	Added = Fields -- OldFields,
	case length(Added) > 0 of
	true -> add_column(File, Added, Columns);
	false -> ignore
	end,
	
	%TODO:
	Writes = 
	lists:foldl(fun({Column, Vales}) -> 

	end, Columns),
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
		file:pwrite(File, Writes);

row2column(Rows) ->
	ok.

lines(Values) ->
	list_to_binary([<<T:32,V:64/float>> || {T, V} <- Values]).

decode(head, HeadData) ->
    <<"RRDB0002", 0:32, DsCnt:32, DsListData/binary>> = HeadData,
	DsList = decode(dslist, DsCnt, DsListData),
	#head{ver = "RRDB0002", dscnt = DsCnt, dslist = DsList};

decode(dslist, DsCnt, DsListData) ->
	[decode(ds, I, DsListData) || 
		I <- lists:seq(0, DsCnt-1)].

decode(ds, I, DsListData) ->
	Offset = I*45,
	<<_:Offset/binary, DsData:45/binary,_/binary>> = DsListData,
	<<NameLen, NameData:24/binary, Pos:32, LastRow:32,LastUp:32,LastVal/float>> = DsData,
	<<Name:NameLen/binary>> = NameData,
	#ds{name = binary_to_list(Name), pos = Pos, lastrow = LastRow, lastup = LastUp, lastval = LastVal}.

decode_data(Data) ->
	decode_data(Data, []).

decode_data(<<0:32,_/binary>>, Acc) ->
	lists:reverse(Acc);
decode_data(<<>>, Acc);
	lists:reverse(Acc);
decode_data(<<Time:32,Value/float,Data/binary>>, Acc) ->
	decode_data(Data, [{Time, Value}|Acc]).

encode(#head{ver=Ver, dscnt=DsCnt, dslist=DsList} = Head) ->
	DsHead = list_to_binary([encode(Ds) || Ds <- DsList]),
	<<"RRDB0002", 0:32, DsCnt:32, DsHead/binary>>;

encode(#ds{name=Name,pos=Pos,lastrow=Row,lastup=Up,lastVal=Val}) ->
	NameBin = list_to_binary(Name),
	NameLen = size(NameBin),
	ZeroLen = (?MAX_DS_NAME_LEN - NameLen) * 8,
	<<NameLen, NameBin/binary,0:ZeroLen,Pos:32,Row:32,Up:32,Val/float>>;

encode(line, {Time, Values}) ->
    ValBin = l2b([<<V/float>> || V <- Values]),
    <<Time:32, ":", ValBin/binary>>.

