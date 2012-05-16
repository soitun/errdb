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
	[application:start(App) || App <- [crypto, compiler, 
		syntax_tools, lager, extlib, elog]],
    case erts_version_check() of
    ok ->
        {ok, SupPid} = errdb_sup:start_link(),
        true = register(errdb, self()),
        ?INFO_MSG("errdb is running~n"),
        {ok, SupPid};
    Error ->
        Error
    end.

erts_version_check() ->
    FoundVer = erlang:system_info(version),
    case errdb_misc:version_compare(?ERTS_MINIMUM, FoundVer, lte) of
	true  -> ok;
	false -> {error, {erlang_version_too_old,
					  {found, FoundVer}, {required, ?ERTS_MINIMUM}}}
    end.

stop(_State) ->
	ok.

