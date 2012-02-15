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
    Req = list_to_tuple(binary:split(Line, [<<" ">>,<<"\r\n">>], [global])),
    request(Req);

request({<<"insert">>, Key, Time, Value, <<>>}) ->
    if
    Value == <<>> ->
        ?ERROR("null insert: ~p", [Key]);
    true ->
        try errdb:insert(Key, b2i(Time), Value) catch
        _:Error -> ?ERROR("error insert:~p, ~p", [Error, Value])
        end
    end,
    "OK\r\n";

request({<<"last">>, Key, <<>>}) ->
    case errdb:last(Key) of
    {ok, Fields, Record} -> 
        Head = ["time: ", string:join(Fields, ",")],
        Line = line(Record),
        list_to_binary([Head, "\r\n", Line, "\r\nEND\r\n"]);
    {error, Reason} ->
        "ERROR: " ++ atom_to_list(Reason) ++ "\r\n"
    end;

request({<<"fetch">>, Key, Begin, End, <<>>}) ->
	case errdb:fetch(Key, b2i(Begin), b2i(End)) of
    {ok, Fields, Records} -> 
        Head = ["time: ", string:join(Fields, ",")],
        Lines = string:join([line(Record) || Record <- Records], "\r\n"),
        list_to_binary([Head, "\r\n", Lines, "\r\nEND\r\n"]);
    {error, Reason} ->
        "ERROR " ++ atom_to_list(Reason) ++ "\r\n"
	end;

request({<<"delete">>, Key, <<>>}) ->
    ok = errdb:delete(Key),
    "OK\r\n";

request(Req) ->
    ?ERROR("bad request: ~p", [Req]),
    "ERROR: bad request\r\n".

