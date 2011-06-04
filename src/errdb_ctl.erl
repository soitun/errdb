%% @author Ery Lee<ery.lee@opengoss.com>
%% @copyright www.opengoss.com

%% @doc errdb controller
-module(errdb_ctl).

-include("elog.hrl").

-include("errdb.hrl").

-export([start/0,
         init/0,
         process/1,
         register_commands/3,
         unregister_commands/3]).

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

init() ->
    ets:new(errdb_ctl_cmds, [named_table, set, public]).

process(["status"]) ->
    {InternalStatus, ProvidedStatus} = init:get_status(),
    ?PRINT("Node ~p is ~p. Status: ~p~n",
              [node(), InternalStatus, ProvidedStatus]),
    case lists:keysearch(errdb, 1, application:which_applications()) of
        false ->
            ?PRINT("errdb is not running~n", []),
            ?STATUS_ERROR;
        {value,_Version} ->
            ?PRINT("errdb is running~n", []),
            ?STATUS_SUCCESS
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
	 {"restart", "restart errdb"}] ++
	ets:tab2list(errdb_ctl_cmds),
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

register_commands(CmdDescs, _Module, _Function) ->
    ets:insert(errdb_ctl_cmds, CmdDescs),
    ok.

unregister_commands(CmdDescs, _Module, _Function) ->
    lists:foreach(fun(CmdDesc) ->
			  ets:delete_object(errdb_ctl_cmds, CmdDesc)
		  end, CmdDescs),
    ok.

