-module(errdb_socket).

-include("elog.hrl").

-define(TIMEOUT, 3600000).

-export([start/1, 
        loop/1, 
        stop/0]).

%% External API
start(Options) ->
    ?INFO("errdb_socket is started...[ok]", []),
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
    errdb:insert(Key, b2i(Time), Value),
    "OK\n";

request({<<"last">>, Key, <<>>}) ->
    case errdb:last(Key) of
    {ok, Fields, Record} -> 
        Head = ["time:", string:join(Fields, ",")],
        Line = line(Record),
        list_to_binary([Head, "\n", Line, "\nEND\n"]);
    {error, Reason} ->
        "ERROR: " ++ atom_to_list(Reason) ++ "\n"
    end;

request({<<"fetch">>, Key, Begin, End, <<>>}) ->
	case errdb:fetch(Key, b2i(Begin), b2i(End)) of
    {ok, Fields, Records} -> 
        Head = ["time:", string:join(Fields, ",")],
        Lines = string:join([line(Record) || Record <- Records], "\n"),
        list_to_binary([Head, "\n", Lines, "\nEND\n"]);
    {error, Reason} ->
        "ERROR" ++ atom_to_list(Reason) ++ "\n"
	end;

request({<<"delete">>, Key, <<>>}) ->
    ok = errdb:delete(Key),
    "OK\n";

request(Req) ->
    ?ERROR("bad request: ~p", [Req]),
    "ERROR: bad request\n".

line({Time, Values}) ->
    Line = string:join([extbif:to_list(V) || V <- Values], ","),
    string:join([extbif:to_list(Time), Line], ":").

b2i(B) when is_binary(B) ->
    list_to_integer(binary_to_list(B)).

