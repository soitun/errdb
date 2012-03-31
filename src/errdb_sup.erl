%%%----------------------------------------------------------------------
%%% File    : errdb_sup.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Errdb supervisor
%%% Created : 03 Jun. 2011
%%% License : http://www.opengoss.com/license
%%%
%%% Copyright (C) 2011, www.opengoss.com
%%%----------------------------------------------------------------------
-module(errdb_sup).

-author('<ery.lee@gmail.com>').

-import(proplists, [get_value/2, get_value/3]).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Env = application:get_all_env(),
	PoolSize = get_value(pool, Env, 8),
    Errdbs = [worker(Id, Env) || Id <- lists:seq(1, PoolSize)],

	%% Httpd config
	HttpdConf = get_value(httpd, Env), 
	%% Httpd 
    Httpd = {errdb_httpd, {errdb_httpd, start, [HttpdConf]},
           permanent, 5000, worker, [errdb_httpd]},

	%% Socket config
	SocketConf = get_value(socket, Env), 
	%% Socket
    Socket = {errdb_socket, {errdb_socket, start, [SocketConf]},
           permanent, 5000, worker, [errdb_socket]},

    %%system monitor
    Monitor = {errdb_monitor, {errdb_monitor, start_link, []},
            permanent, 5000, worker, [errdb_monitor]},

    {ok, {{one_for_all, 10, 1000}, Errdbs ++ [Httpd, Socket, Monitor]}}.

worker(Id, Env) ->
	{errdb:name(Id), {errdb, start_link, [Id, Env]},
	   permanent, 5000, worker, [errdb]}.

