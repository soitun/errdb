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

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, StoreOpts} = application:get_env(store),
    Stores = [begin 
        Name = l2a("errdb_store_" ++ i2l(I)),
        {Name, {errdb_store, start_link, [Name, StoreOpts]},
           permanent, 100, worker, [errdb_store]}
    end || I <- lists:seq(1, 4)],
    {ok, JournalOpts} = application:get_env(journal),
    Journal = {errdb_journal, {errdb_journal, start_link, [JournalOpts]},
           permanent, 100, worker, [errdb_journal]},
    {ok, DbOpts} = application:get_env(rrdb),
    Errdb = {errdb, {errdb, start_link, [DbOpts]},
           permanent, 100, worker, [errdb_hrlog]},
    {ok, {{one_for_all, 0, 1}, [Journal] ++ Stores ++ [Errdb]}}.

l2a(L) ->
    list_to_atom(L).

i2l(I) ->
    integer_to_list(I).
