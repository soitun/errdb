-module(errdb_socket).

-include_lib("elog/include/elog.hrl").

-import(lists, [concat/1]).

-import(errdb_lib, [decode/1, encode/1, encode/2]).

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

trim(Line) ->
	[H|_] = binary:split(Line, [<<"\r\n">>], [global, trim]), H.

loop(Socket) ->
    inet:setopts(Socket, [{packet, line},{keepalive, true}]),
    case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
    {ok, Line} when is_binary(Line) -> 
        Reply = request(binary_to_list(trim(Line))),
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

request(Line) when is_list(Line) ->
	request(list_to_tuple(string:tokens(Line, " ")));

request({"insert", Object, Time, Metrics}) ->
	?INFO("insert ~p ~p ~p", [Object, Time, Metrics]),
	errdb:insert(Object, list_to_integer(Time), decode(Metrics)),
	%try errdb:insert(Object, list_to_integer(Time), decode(Metrics)) catch
	%_:Error -> ?ERROR("error insert:~p, ~p", [Error, Metrics])
	%end,
    "OK\r\n";

request({"last", Object}) ->
    case errdb:last(Object) of
    {ok, Time, Fields, Values} -> 
		Head = string:join(Fields, ","),
		Line = errdb_lib:line(Time, Values),
		["time:", Head, "\r\n", Line, "\r\nEND\r\n"];
    {error, Reason} ->
        ["ERROR: ", atom_to_list(Reason), "\r\n"]
    end;

request({"last", Object, Fields}) ->
    case errdb:last(Object, string:tokens(Fields, ",")) of
    {ok, Time, Values} -> 
		Line = errdb_lib:line(Time, Values),
		["time:", Fields, "\r\n", Line, "\r\nEND\r\n"];
    {error, Reason} ->
        ["ERROR: ", atom_to_list(Reason), "\r\n"]
    end;

request({"fetch", Object, Fields, Begin, End}) ->
	case errdb:fetch(Object, string:tokens(Fields, ","), 
		list_to_integer(Begin), list_to_integer(End)) of
    {ok, Records} -> 
		Head = ["time:", Fields],
		Lines = string:join([errdb_lib:line(Time, Values) 
			|| {Time, Values} <- Records], "\r\n"),
		[Head, "\r\n", Lines, "\r\nEND\r\n"];
    {error, Reason} ->
        ["ERROR ", atom_to_list(Reason), "\r\n"]
	end;

request(Req) ->
    ?ERROR("bad request: ~p", [Req]),
    "ERROR: bad request\r\n".

