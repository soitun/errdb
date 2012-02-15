%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc Errdb application.

-module(errdb_app).

-include_lib("elog/include/elog.hrl").

-include("errdb.hrl").

-export([start/0, stop/0]).

-behavior(application).
%callback
-export([start/2, stop/1]).

%%@spec start() -> ok
%%@doc Start the errdb server
start() -> 
    init_elog(),
	application:start(crypto),
	application:start(core),
	application:start(errdb).

init_elog() ->
    {ok, [[LogLevel]]} = init:get_argument(log_level),
    {ok, [[LogPath]]} = init:get_argument(log_path),
	elog:init(list_to_integer(LogLevel), LogPath).

%%@spec stop() -> ok
%%@doc Stop the errdb server
stop() -> 
    application:stop(errdb),
	application:stop(core),
	application:stop(crypto).

start(_Type, _Args) ->
    case erts_version_check() of
    ok ->
        {ok, SupPid} = errdb_sup:start_link(),
        true = register(errdb, self()),
        io:format("~nerrdb is running~n"),
        {ok, SupPid};
    Error ->
        Error
    end.


stop(_State) ->
	ok.

erts_version_check() ->
    FoundVer = erlang:system_info(version),
    case errdb_misc:version_compare(?ERTS_MINIMUM, FoundVer, lte) of
        true  -> ok;
        false -> {error, {erlang_version_too_old,
                          {found, FoundVer}, {required, ?ERTS_MINIMUM}}}
    end.
