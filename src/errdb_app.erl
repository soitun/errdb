%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc Errdb application.

-module(errdb_app).

-export([start/0, stop/0]).

-behavior(application).
%callback
-export([start/2, stop/1]).

%%@spec start() -> ok
%%@doc Start the errdb server
start() -> 
    init_elog(),
	application:start(errdb).

init_elog() ->
    {ok, [[LogLevel]]} = init:get_argument(log_level),
    {ok, [[LogPath]]} = init:get_argument(log_path),
	elog:init(list_to_integer(LogLevel), LogPath).

%%@spec stop() -> ok
%%@doc Stop the errdb server
stop() -> 
    application:stop(errdb).

start(_Type, _Args) ->
	errdb_sup:start_link().

stop(_State) ->
	ok.

