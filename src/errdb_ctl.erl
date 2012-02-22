%% @author Ery Lee<ery.lee@opengoss.com>
%% @copyright www.opengoss.com

%% @doc errdb controller
-module(errdb_ctl).

-include_lib("elog/include/elog.hrl").

-include("errdb.hrl").

-export([start/0,
         process/1]).

start() ->
    case init:get_plain_arguments() of
	[SNode | Args] ->
	    SNode1 = case string:tokens(SNode, "@") of
		[_Node, _Server] ->
		    SNode;
		_ ->
		    case net_kernel:longnames() of
			 true ->
			     SNode ++ "@" ++ inet_db:gethostname() ++
				      "." ++ inet_db:res_option(domain);
			 false ->
			     SNode ++ "@" ++ inet_db:gethostname();
			 _ ->
			     SNode
		     end
	    end,
	    Node = list_to_atom(SNode1),
	    Status = case rpc:call(Node, ?MODULE, process, [Args]) of
			 {badrpc, Reason} ->
			     ?PRINT("RPC failed on the node ~p: ~p~n",
				       [Node, Reason]),
			     ?STATUS_BADRPC;
			 S ->
			     S
		     end,
	    halt(Status);
	_ ->
	    print_usage(),
	    halt(?STATUS_USAGE)
    end.

process(["status"]) ->
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
        ?PRINT("errdb is not running~n", []),
        ?STATUS_ERROR;
    {value, Version} ->
        ?PRINT("errdb ~p is running~n", [Version]),
        ?STATUS_SUCCESS
    end;

process(["cluster", Node]) ->
	case net_adm:ping(list_to_atom(Node)) of
	pong ->
		?PRINT("cluster with ~p successfully.~n", [Node]),
		?STATUS_SUCCESS;
	pang ->
        ?PRINT("failed to cluster with ~p~n", [Node]),
        ?STATUS_ERROR
	end;

process(["stop"]) ->
    init:stop(),
    ?STATUS_SUCCESS;

process(["restart"]) ->
    init:restart(),
    ?STATUS_SUCCESS;

process(_Args) ->
    print_usage(),
    ?STATUS_USAGE.

print_usage() ->
    CmdDescs =
	[{"status", "get errdb status"},
	 {"stop", "stop errdb "},
	 {"restart", "restart errdb"}],
    MaxCmdLen =
	lists:max(lists:map(
		    fun({Cmd, _Desc}) ->
			    length(Cmd)
		    end, CmdDescs)),
    NewLine = io_lib:format("~n", []),
    FmtCmdDescs =
	lists:map(
	  fun({Cmd, Desc}) ->
		  ["  ", Cmd, string:chars($\s, MaxCmdLen - length(Cmd) + 2),
		   Desc, NewLine]
	  end, CmdDescs),
    ?PRINT(
      "Usage: errdbctl [--node nodename] command [options]~n"
      "~n"
      "Available commands in this errdb node:~n"
      ++ FmtCmdDescs ++
      "~n"
      "Examples:~n"
      "  errdbctl stop~n"
      "  errdbctl --node errdb@host restart~n",
     []).

