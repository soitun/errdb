%% @author Ery Lee<ery.lee@opengoss.com>
%% @copyright www.opengoss.com

%% @doc errdb controller
-module(errdb_ctl).

-include_lib("elog/include/elog.hrl").

-include("errdb.hrl").

-compile(export_all).

cluster(Node) ->
	case net_adm:ping(list_to_atom(Node)) of
	pong ->
		?PRINT("cluster with ~p successfully.~n", [Node]);
	pang ->
        ?PRINT("failed to cluster with ~p~n", [Node])
	end.

status() ->
    Infos = lists:flatten(errdb:info()),
    [?PRINT("process ~p: ~n~p~n", [Name, Info]) || {Name, Info} <- Infos],
    Tabs = ets:all(),
    ErrdbTabs = lists:filter(fun(Tab) -> 
        if
        is_atom(Tab) ->
            lists:prefix("errdb", atom_to_list(Tab));
        true ->
            false
        end
    end, Tabs),
    [?PRINT("table ~p:~n~p~n", [Tab, ets:info(Tab)]) || Tab <- ErrdbTabs],
    {InternalStatus, ProvidedStatus} = init:get_status(),
    ?PRINT("Node ~p is ~p. Status: ~p~n",
              [node(), InternalStatus, ProvidedStatus]),
    case lists:keysearch(errdb, 1, application:which_applications()) of
    false ->
        ?PRINT("errdb is not running~n", []);
    {value, Version} ->
        ?PRINT("errdb ~p is running~n", [Version])
    end.

