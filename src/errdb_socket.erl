-module(errdb_socket).

-include_lib("elog/include/elog.hrl").

-import(errdb_misc, [b2i/1, line/1]).

-export([start/1, 
        loop/1, 
        stop/0]).

-define(TIMEOUT, 3600000).

%% External API
start(Options) ->
    io:format("~nerrdb_socket is started.~n"),
    mochiweb_socket_server:start([{name, ?MODULE}, 
        {loop, fun loop/1} | Options]).

stop() ->
    mochiweb_socket_server:stop(?MODULE).

loop(Socket) ->
    inet:setopts(Socket, [{packet, line},{keepalive, true}]),
    case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
    {ok, Line} -> 
        Reply = request(Line),
        case gen_tcp:send(Socket, Reply) of
        ok -> loop(Socket);
        _ -> exit(normal)
        end;
    {error, closed} ->
        gen_tcp:close(Socket),
        exit(normal);
    {error, timeout} ->
        gen_tcp:close(Socket),
        exit(normal);
    Other ->
        ?ERROR("invalid request: ~p", [Other]),
        gen_tcp:close(Socket),
        exit(normal)
    end.

request(Line) when is_binary(Line) ->
	[Line1|_] = binary:split(Line, [<<"\r\n">>], [global]),
    Req = list_to_tuple(binary:split(Line1, [<<" ">>], [global])),
	request(Req);

request({<<"insert">>, Obj, Grp, _T, <<>>}) ->
	?ERROR("null insert: ~p", [{Obj, Grp}]),
	<<"ERROR: no metrics">>;

request({<<"insert">>, Obj, Grp, Time, Value}) ->
	try errdb:insert({Obj, Grp}, b2i(Time), Value) catch
	_:Error -> ?ERROR("error insert:~p, ~p", [Error, Value])
	end,
    "OK\r\n";

request({<<"insert">>, _Key, _Time, <<>>}) ->
	<<"ERROR: no metrics">>;

request({<<"insert">>, Key, Time, Value}) ->
	try errdb:insert(Key, b2i(Time), Value) catch
	_:Error -> ?ERROR("error insert:~p, ~p", [Error, Value])
	end,
    "OK\r\n";

request({<<"last">>, Obj, Grp}) ->
    case errdb:last(Obj, Grp) of
    {ok, Fields, Record} -> 
		encode_records(Fields, [Record]);
    {error, Reason} ->
        "ERROR: " ++ atom_to_list(Reason) ++ "\r\n"
    end;

request({<<"last">>, Key}) ->
    case errdb:last(Key) of
    {ok, Fields, Record} -> 
		encode_records(Fields, [Record]);
    {error, Reason} ->
        "ERROR: " ++ atom_to_list(Reason) ++ "\r\n"
    end;

request({<<"fetch">>, Obj, Grp, Begin, End}) ->
	case errdb:fetch(Obj, Grp, b2i(Begin), b2i(End)) of
    {ok, Fields, Records} -> 
		encode_records(Fields, Records);
    {error, Reason} ->
        "ERROR " ++ atom_to_list(Reason) ++ "\r\n"
	end;

request({<<"fetch">>, Key, Begin, End}) ->
	case errdb:fetch(Key, b2i(Begin), b2i(End)) of
    {ok, Fields, Records} -> 
		encode_records(Fields, Records);
    {error, Reason} ->
        "ERROR " ++ atom_to_list(Reason) ++ "\r\n"
	end;

request({<<"delete">>, Obj, Grp}) ->
    ok = errdb:delete(Obj, Grp),
    "OK\r\n";

request({<<"delete">>, Key}) ->
    ok = errdb:delete(Key),
    "OK\r\n";

request(Req) ->
    ?ERROR("bad request: ~p", [Req]),
    "ERROR: bad request\r\n".

encode_records(Fields, Records) ->
	Head = ["time: ", string:join(Fields, ",")],
	Lines = string:join([line(Record) || Record <- Records], "\r\n"),
	list_to_binary([Head, "\r\n", Lines, "\r\nEND\r\n"]).
