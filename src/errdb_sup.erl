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

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %rrdb cluster
	{ok, Pool} = application:get_env(pool_size),
    {ok, Opts} = application:get_env(rrdb),
    Errdbs = [worker(Id, Opts) || Id <- lists:seq(1, Pool)],

	%% Httpd config
	{ok, HttpdConf} = application:get_env(httpd), 
	%% Httpd 
    Httpd = {errdb_httpd, {errdb_httpd, start, [HttpdConf]},
           permanent, 10, worker, [errdb_httpd]},

	%% Socket config
	{ok, SocketConf} = application:get_env(socket), 
	%% Socket
    Socket = {errdb_socket, {errdb_socket, start, [SocketConf]},
           permanent, 10, worker, [errdb_socket]},

    %%system monitor
    Monitor = {errdb_monitor, {errdb_monitor, start_link, []},
            permanent, 10, worker, [errdb_monitor]},

    {ok, {{one_for_one, 10, 1000}, Errdbs ++ [Httpd, Socket, Monitor]}}.

worker(Id, Opts) ->
	{errdb:name(Id), {errdb, start_link, [Id, Opts]},
	   permanent, 5000, worker, [errdb]}.

