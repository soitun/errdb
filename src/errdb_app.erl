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
    case errdb_misc:version_compare(?ERTS_MINIMUM, FoundVer, lte) of
	true  -> ok;
	false -> {error, {erlang_version_too_old,
					  {found, FoundVer}, {required, ?ERTS_MINIMUM}}}
    end.

stop(_State) ->
	ok.

