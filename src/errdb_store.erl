%%%----------------------------------------------------------------------
%%% File    : errdb_store.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : File Storage
%%% Created : 03 Apr. 2010
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2012, www.opengoss.com
%%%----------------------------------------------------------------------
-module(errdb_store).

-author('ery.lee@gmail.com').

-compile(export_all).

-include_lib("elog/include/elog.hrl").

-import(extbif, [zeropad/1]).

-import(lists, [concat/1, reverse/1]).

-behavior(gen_server).

-export([start_link/2,
		name/1,
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

-define(RRDB_VER, <<"RRDB0002">>).

-define(HEAD_SIZE, 4096).

-define(DS_NAME_SIZE, 24).

-define(DS_HEAD_SIZE, (?DS_NAME_SIZE+21)).

-define(MAX_COLUMNS, 80).

-define(PAGE_ROWS, 600).

-define(PAGE_START, 4096).

-define(PAGE_SIZE, (?PAGE_ROWS*12)).

-define(OPEN_MODES, [binary, raw, {read_ahead, 4096}]).

-record(state, {dbdir}).

-record(rrdb_head, {ver = ?RRDB_VER, dscnt, dslist}).

-record(rrdb_ds, {name, idx, rowptr=0, lastup=0, lastval=0.0}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Id, Dir) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Id, Dir],
		[{spawn_opt, [{min_heap_size, 204800}]}]).

name(Id) ->
    list_to_atom("errdb_store_" ++ integer_to_list(Id)).

read(DbDir, Key, Fields) ->
	case file:open(filename(DbDir, Key), [read | ?OPEN_MODES]) of
	{ok, File} ->
		case read_head(File) of
		{ok, Head} -> 
			case check_fields(Fields, Head#rrdb_head.dslist) of
			ok -> 
				{ok, read_data(File, Head, Fields)};
			Error -> 
				Error
			end;
		Error -> 
			Error
		end;
	{error, enoent} ->
		{ok, []};
    {error, Error} -> 
        {error, Error}
	end.

check_fields([], _DsList) ->
	ok;
check_fields([F|Fields], DsList) ->
	case lists:keysearch(F, 2, DsList) of
	{value, _} -> check_fields(Fields, DsList);
	false -> {error, badfield}
	end.
	
read_head(File) ->
	case file:read(File, 4096) of
    {ok, <<"RRDB0002", _/binary>> = HeadData} ->
		{ok, decode_head(HeadData)};
	{ok, _} ->
		{error, badhead};
	eof ->
		{error, nofile};
	{error, Reason} ->
		{error, Reason}
	end.

read_data(File, Head, Fields) ->
	Reads = [ {get_ds_pos(Ds), ?PAGE_SIZE} || 
		Ds <- [get_ds(F, Head) || F <- Fields] ],
	{ok, Pages} = file:pread(File, Reads),
	lists:zip(Fields, [decode_page(Page) || Page <- Pages]).

get_ds(Field, #rrdb_head{dslist=DsList}) ->
	case lists:keysearch(Field, 2, DsList) of
	{value, Ds} -> Ds;
	false -> false
	end.

get_ds_pos(#rrdb_ds{idx = Idx}) ->
	?HEAD_SIZE + Idx * ?PAGE_SIZE.
    
write(Pid, Key, Rows) when length(Rows) > 0 ->
    gen_server2:cast(Pid, {write, Key, Rows}).

delete(Pid, Key) ->
    gen_server2:cast(Pid, {delete, Key}).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Id, Dir]) ->
    ?INFO("~p is started.", [name(Id)]),
	DbDir = Dir ++ "/" ++ integer_to_list(Id),
    {ok, #state{dbdir = DbDir}}.

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
    {stop, {error, {badreq, Req}}, State}.

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
    case file:open(FileName, [read, write | ?OPEN_MODES]) of
	{ok, File} ->
		Columns = errdb_lib:transform(Rows),
		case read_head(File) of
		{ok, Head} ->
			case check_columns(Head, Columns) of
			{ok, same} ->
				write_rrdb(File, Head, Columns);
			{ok, alter, Names} ->
				NewHead = alter_rrdb(File, Head, Names),
				write_rrdb(File, NewHead, Columns);
			{error, Error} ->
				?ERROR("check columns error: ~p", [Error])
			end;
		{error, nofile} ->
			create_rrdb(File, Columns);
		{error, Error} ->
			?ERROR("failed to read head: ~p", [Error])
		end,
		file:close(File);
	{error, Reason} -> 
        ?ERROR("failed to open '~p': ~p", [FileName, Reason])
	end,
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
%%% Key: dn:grp
%%--------------------------------------------------------------------
filename(Dir, Key) ->
    Path = binary:replace(list_to_binary(Key), 
		[<<",">>, <<":">>], <<"/">>, [global]),
    concat([Dir, "/", binary_to_list(Path), ".rrdb"]).

create_rrdb(_File, Columns) when length(Columns) > ?MAX_COLUMNS ->
	?ERROR("too many columns: ~p, cannot create rrdb file!", [length(Columns)]);

create_rrdb(File, Columns) ->
	?INFO("create rrdb with data: ~n~p", [Columns]),
	{DsCnt, DsList, Writes} =
	lists:foldl(fun({Col, Values}, {Idx, DsAcc, WirteAcc}) -> 
		SortedValues = lists:sort(Values),
		{LastUp, LastVal} = lists:last(SortedValues),
		Ds = #rrdb_ds{name = Col, idx = Idx, 
			rowptr = length(Values),
			lastup = LastUp,
			lastval = LastVal}, 
		PagePos = ?PAGE_START+Idx*?PAGE_SIZE,
		PageData = encode_value(SortedValues),
		Write = {PagePos, PageData},
		{Idx+1, [Ds|DsAcc], [Write|WirteAcc]}
	end, {0, [], []}, Columns),
	Head = #rrdb_head{ver = <<"RRDB0002">>, dscnt = DsCnt,
					dslist = lists:reverse(DsList)},
	file:write(File, encode_head(Head)),
	file:pwrite(File, Writes).

alter_rrdb(File, Head, Names) ->
	#rrdb_head{dscnt = DsCnt, dslist=DsList} = Head,
	NewDs = fun(Idx) -> #rrdb_ds{name=lists:nth(Idx, Names), idx=DsCnt+Idx-1} end,
	NewDsCnt = DsCnt + length(Names), 
	NewDsList = [ NewDs(I) || I <- lists:seq(1, length(Names)) ],

	NewDsPos = 16 + ?DS_HEAD_SIZE * DsCnt,
	NewDsData = list_to_binary([encode_ds(Ds) || Ds <- NewDsList]),
	Writes = [{12, <<NewDsCnt:32>>}, {NewDsPos, NewDsData}],
	file:pwrite(File, Writes),
	Head#rrdb_head{dscnt = NewDsCnt, dslist=DsList++NewDsList}.

check_columns(#rrdb_head{dslist=DsList}, Columns) ->
	OldCols = [Name || #rrdb_ds{name=Name} <- DsList],
	NewCols = [Name || {Name, _} <- Columns],
	case NewCols -- OldCols of
	[] -> 
		{ok, same};
	Added when length(Added) =< (length(OldCols) div 2) ->
		{ok, alter, Added};
	Added ->
		{error, {too_many_changed, Added}}
	end.

write_rrdb(File, Head, Columns) ->
	Fields = [Name || {Name,_} <- Columns],
	DsList = [get_ds(F, Head) || F <- Fields],
	{HeadWrites, DataWrites} = 
	lists:foldl(fun(Ds, {HeadAcc, DataAcc}) -> 
		#rrdb_ds{name=Name,idx=Idx,rowptr=RowPtr} = Ds,
		Values = lists:sort(proplists:get_value(Name, Columns)), 
		{LastUp, LastVal} = lists:last(Values),

		?INFO("before write: rowptr=~p,", [RowPtr]),
		
		{NewRowPtr, PageWrites} = write_page(Idx, RowPtr, Values),
		
		?INFO("after write: rowptr=~p, ~n~p", [NewRowPtr, PageWrites]),

		DsLoc = ds_head_pos(Idx) + 1 + ?DS_NAME_SIZE + 4,
		DsData = <<NewRowPtr:32, LastUp:32, LastVal:64/float>>,

		?INFO("~p", [{DsLoc, DsData}]),
		
		{[{DsLoc, DsData}|HeadAcc], [PageWrites|DataAcc]}
	end, {[], []}, DsList),
	file:pwrite(File, HeadWrites),
	file:pwrite(File, lists:flatten(DataWrites)).

write_page(Idx, RowStart, Values) ->
	Length = length(Values),
	RowEnd = RowStart + Length,
	PagePos = ?PAGE_START + Idx * ?PAGE_SIZE,
	RowPos = PagePos + RowStart*12,
	if
	RowEnd < ?PAGE_ROWS ->
		{RowEnd, [{RowPos, encode_value(Values)}]};
	true ->
		OverLen = RowEnd - ?PAGE_ROWS,
		Writes = [{PagePos, encode_value(lists:sublist(Values, Length-OverLen, OverLen))},
				  {RowPos, encode_value(lists:sublist(Values, Length-OverLen))}],
		{OverLen, Writes}
	end.

decode_head(HeadData) ->
    <<"RRDB0002", 0:32, DsCnt:32, DsHead/binary>> = HeadData,
	DsList = [decode_ds(I, DsHead) || I <- lists:seq(0, DsCnt-1)],
	#rrdb_head{dscnt = DsCnt, dslist = DsList}.

decode_ds(I, DsHeadData) ->
	Offset = I*?DS_HEAD_SIZE,
	<<_:Offset/binary, DsData:45/binary,_/binary>> = DsHeadData,
	<<NameLen, NameData:?DS_NAME_SIZE/binary, Idx:32, RowPtr:32,LastUp:32,LastVal:64/float>> = DsData,
	<<Name:NameLen/binary, _/binary>> = NameData,
	#rrdb_ds{name = binary_to_list(Name), idx = Idx, rowptr = RowPtr, lastup = LastUp, lastval = LastVal}.

decode_page(Page) ->
	decode_page(Page, []).

decode_page(<<0:32,_/binary>>, Acc) ->
	reverse(Acc);
decode_page(<<>>, Acc) ->
	reverse(Acc);
decode_page(<<Time:32,Value/float,Data/binary>>, Acc) ->
	decode_page(Data, [{Time, Value}|Acc]).

encode_head(#rrdb_head{ver=Ver, dscnt=DsCnt, dslist=DsList}) ->
	DsHead = list_to_binary([encode_ds(Ds) || Ds <- DsList]),
	<<Ver/binary, 0:32, DsCnt:32, DsHead/binary>>.

encode_ds(#rrdb_ds{name=Name,idx=Idx,rowptr=Row,lastup=Up,lastval=Val}) ->
	NameBin = list_to_binary(Name),
	NameLen = size(NameBin),
	ZeroLen = (?DS_NAME_SIZE - NameLen) * 8,
	<<NameLen, NameBin/binary,0:ZeroLen,Idx:32,Row:32,Up:32,Val:64/float>>.

encode_value(List) when is_list(List) ->
	list_to_binary([encode_value(One) || One <- List]);

encode_value({T, V}) ->
	<<T:32,V:64/float>>.

ds_head_pos(Idx) ->
	16 + ?DS_HEAD_SIZE * Idx.


