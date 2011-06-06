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

-import(errdb, [l2a/1, i2l/1]).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, DbOpts} = application:get_env(rrdb),
    Errdbs = [begin 
        Name = l2a("errdb_" ++ i2l(Id)),
        Opts = [{id, Id}|DbOpts],
        {Name, {errdb, start_link, [Name, Opts]},
           permanent, 100, worker, [errdb]}
    end || Id <- lists:seq(1, 4)],
    {ok, JournalOpts} = application:get_env(journal),
    Journal = {errdb_journal, {errdb_journal, start_link, [JournalOpts]},
           permanent, 100, worker, [errdb_journal]},

	%% Httpd config
	{ok, HttpdConf} = application:get_env(httpd), 
	%% Httpd 
    Httpd = {errdb_httpd, {errdb_httpd, start, [HttpdConf]},
           permanent, 10, worker, [errdb_httpd]},
    {ok, {{one_for_all, 0, 1}, [Httpd,Journal|Errdbs]}}.

