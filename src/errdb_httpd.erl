%% @author Ery Lee<ery.lee@opengoss.com>
%% @copyright www.opengoss.com

%% @doc Http server for errdb.
-module(errdb_httpd).

-include("elog.hrl").

-export([start/1, 
        loop/1, 
        stop/0]).

%% External API
start(Options) ->
    ?INFO("errdb_httpd is started...[ok]", []),
    mochiweb_http:start([{name, ?MODULE}, {loop, fun loop/1} | Options]).

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req) ->
    Method = Req:get(method),
	Path = list_to_tuple(string:tokens(Req:get(path), "/")),
    ?INFO("HTTP Req: ~p", [Path]),
	handle(Method, Path, Req).

handle('GET', {"rrdb", Key, "last"}, Req) ->
	case errdb:last(list_to_binary(Key)) of
    {ok, Fields, Record} -> 
        ?INFO("~p, ~p", [Fields, Record]),
        Head = string:join(Fields, ","),
        Line = line(Record),
        ?INFO("line: ~p", [Line]),
        Resp = list_to_binary(["time:", Head, "\n", Line]),
        Req:ok({"text/plain", Resp});
    {error, Reason} ->
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle('GET', {"rrdb", Key, Range}, Req) ->
    [Begin,End|_] = string:tokens(Range, "-"),
	case errdb:fetch(list_to_binary(Key), 
        list_to_integer(Begin), list_to_integer(End)) of
    {ok, Fields, Records} -> 
        Head = string:join(Fields, ","),
        Lines = string:join([line(Record) || Record <- Records], "\n"),
        Resp = list_to_binary(["time:", Head, "\n", Lines]),
        Req:ok({"text/plain", Resp});
    {error, Reason} ->
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle(_Other, _Path, Req) ->
	Req:respond({500, [], <<"unsupported request">>}). 

line({Time, Values}) ->
    Line = string:join([extbif:to_list(V) || V <- Values], ","),
    string:join([extbif:to_list(Time), Line], ":").

