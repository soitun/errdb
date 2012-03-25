%% @author author <ery.lee@gmail.com>
%% @copyright 2012 www.opengoss.com.

%% @doc Errdb application.

-module(errdb_app).

-include("errdb.hrl").

-include_lib("elog/include/elog.hrl").

-export([start/0]).

-behavior(application).
%callback
-export([start/2, stop/1]).

start() ->
	application:start(errdb).

start(_Type, _Args) ->
    init_elog(),
	application:start(crypto),
	application:start(extlib),
	application:start(sqlite3),
    case erts_version_check() of
    ok ->
        {ok, SupPid} = errdb_sup:start_link(),
        true = register(errdb, self()),
        io:format("~nerrdb is running~n"),
        {ok, SupPid};
    Error ->
        Error
    end.

init_elog() ->
    {ok, [[LogLevel]]} = init:get_argument(log_level),
    {ok, [[LogPath]]} = init:get_argument(log_path),
	elog:init(list_to_integer(LogLevel), LogPath).

erts_version_check() ->
    FoundVer = erlang:system_info(version),
    case version_compare(?ERTS_MINIMUM, FoundVer, lte) of
	true  -> ok;
	false -> {error, {erlang_version_too_old,
					  {found, FoundVer}, {required, ?ERTS_MINIMUM}}}
    end.

stop(_State) ->
	ok.

version_compare(A, B, lte) ->
    case version_compare(A, B) of
        eq -> true;
        lt -> true;
        gt -> false
    end;
version_compare(A, B, gte) ->
    case version_compare(A, B) of
        eq -> true;
        gt -> true;
        lt -> false
    end;
version_compare(A, B, Result) ->
    Result =:= version_compare(A, B).

version_compare(A, A) ->
    eq;
version_compare([], [$0 | B]) ->
    version_compare([], dropdot(B));
version_compare([], _) ->
    lt; %% 2.3 < 2.3.1
version_compare([$0 | A], []) ->
    version_compare(dropdot(A), []);
version_compare(_, []) ->
    gt; %% 2.3.1 > 2.3
version_compare(A,  B) ->
    {AStr, ATl} = lists:splitwith(fun (X) -> X =/= $. end, A),
    {BStr, BTl} = lists:splitwith(fun (X) -> X =/= $. end, B),
    ANum = list_to_integer(AStr),
    BNum = list_to_integer(BStr),
    if ANum =:= BNum -> version_compare(dropdot(ATl), dropdot(BTl));
       ANum < BNum   -> lt;
       ANum > BNum   -> gt
    end.

dropdot(A) -> lists:dropwhile(fun (X) -> X =:= $. end, A).

