%% @author Ery Lee<ery.lee@opengoss.com>
%% @copyright www.opengoss.com

%% @doc Http server for errdb.
-module(errdb_httpd).

-include_lib("elog/include/elog.hrl").

-import(string, [join/2, tokens/2]).

-export([start/1, 
        loop/1, 
        stop/0]).

%% External API
start(Options) ->
    ?INFO_MSG("errdb_httpd is started."),
    mochiweb_http:start([{name, ?MODULE}, {loop, fun loop/1} | Options]).

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req) ->
    Method = Req:get(method),
	Path = list_to_tuple(string:tokens(Req:get(path), "/")),
	handle(Method, Path, Req).

handle('GET', {"rrdb", Key, "last"}, Req) ->
	case errdb:last(Key) of
    {ok, Time, Fields, Values} ->
        Resp = ["TIME:", join(Fields, ","), "\n", errdb_lib:line(Time, Values)],
        Req:ok({"text/plain", Resp});
    {error, Reason} ->
		?ERROR("~p", [Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle('GET', {"rrdb", Key, "last", Fields}, Req) ->
	case errdb:last(Key, tokens(Fields, ",")) of
    {ok, Time, Values} -> 
        Resp = ["TIME:", Fields, "\n", errdb_lib:line(Time, Values)],
        Req:ok({"text/plain", Resp});
    {error, Reason} ->
		?ERROR("~p", [Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle('GET', {"rrdb", Key, Fields, Range}, Req) ->
    [Begin, End] = tokens(Range, "-"),
	case errdb:fetch(Key, tokens(Fields, ","),
        list_to_integer(Begin), list_to_integer(End)) of
    {ok, Records} -> 
        Lines = join([errdb_lib:line(Time, Values) || {Time, Values} <- Records], "\n"),
        Resp = ["TIME:", Fields, "\n", Lines],
        Req:ok({"text/plain", Resp});
    {error, Reason} ->
		?ERROR("~p", [Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle(_Other, _Path, Req) ->
	Req:respond({500, [], <<"unsupported request">>}). 

