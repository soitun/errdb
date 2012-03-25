%%%----------------------------------------------------------------------
%%% File    : errdb_client.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Errdb tcp client
%%% Created : 08 Jun. 2011
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2011, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(errdb_client).

-author('ery.lee@gmail.com').

-include_lib("elog/include/elog.hrl").

-import(lists, [concat/1]).

-import(errdb_lib, [str/1, strnum/1]).

-behavior(gen_fsm).

-export([start_link/0,
		start_link/1, 
        status/1, 
        last/2,
		last/3,
        fetch/4,
        insert/4,
        stop/1]).

%% gen_fsm callbacks
-export([init/1,
		handle_info/3,
		handle_event/3,
		handle_sync_event/4,
		code_change/4,
		terminate/3]).

%% fsm state
-export([connecting/2,
        connecting/3,
        connected/2,
		connected/3]).

-define(TCP_OPTIONS, [binary, 
    {packet, line}, 
    {active, true}, 
    {reuseaddr, true}, 
    {send_timeout, 3000}]).

-define(TIMEOUT, 3000).

-record(state, {host, port, socket, queue, reply = []}).

start_link() ->
	start_link([]).
start_link(Args) ->
    gen_fsm:start_link(?MODULE, [Args], []).

last(Pid, Object) ->
	gen_fsm:sync_send_event(Pid, {last, Object}).

last(Pid, Object, Fields) ->
	gen_fsm:sync_send_event(Pid, {last, Object, Fields}).

fetch(Pid, Object, Begin, End) when is_integer(Begin)
	and is_integer(End) ->
	gen_fsm:sync_send_event(Pid, {fetch, Object, Begin, End}).

insert(Pid, Object, Time, Metrics) when is_integer(Time) ->
    gen_fsm:send_event(Pid, {insert, Object, Time, Metrics}).

status(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, status).

stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop).

init([Args]) ->
    put(inserted, 0),
    put(datetime, {date(), time()}),
    Host = proplists:get_value(host, Args, "localhost"),
    Port = proplists:get_value(port, Args, 7272),
    State = #state{host = Host, port = Port, queue = queue:new()},
    {ok, connecting, State, 0}.

connecting(timeout, State) ->
    connect(State);

connecting(_Event, State) ->
    {next_state, connecting, State}.

connecting(Event, _From, State) ->
    ?ERROR("badevent when connecting: ~p", [Event]),
	{reply, {error, connecting}, connecting, State}.

connect(#state{host = Host, port = Port} = State) ->
    case gen_tcp:connect(Host, Port, ?TCP_OPTIONS, ?TIMEOUT) of
    {ok, Socket} ->
        {next_state, connected, State#state{socket = Socket}};
    {error, Reason} ->
        ?ERROR("tcp connect failure: ~p", [Reason]),
        retry_connect(),
        {next_state, connecting, State#state{socket = undefined}}
    end.

connected({insert, Object, Time, Metrics}, 
	#state{socket = Socket} = State) ->
    Insert = ["insert ", Object, 
		 " ", integer_to_list(Time), 
		 " ", encode(Metrics), "\r\n"],
    gen_tcp:send(Socket, Insert),
    put(inserted, get(inserted)+1),
    {next_state, connected, State}.

connected({last, Object}, From, State) ->
	send_req(["last ", Object, "\r\n"], From, State);

connected({last, Object, Fields}, From, State) ->
	Fields1 = string:join([str(F) || F <- Fields], ","),
	Cmd = ["last ", Object, " ", Fields1, "\r\n"],
	send_req(Cmd, From, State);

connected({fetch, Object, Fields, Begin, End}, From, State) ->
	Fields1 = string:join([str(F) || F <- Fields], ","),
	Cmd = ["fetch", Object, Fields1, integer_to_list(Begin), integer_to_list(End)],
	Cmd1 = [string:join(Cmd, " "), "\r\n"],
	send_req(Cmd1, From, State);

connected(Req, _From, State) ->
	?ERROR("badreq: ~p", [Req]),
    {next_state, connected, State}.

handle_info({timeout, req, Ref}, connected, #state{queue = Q} = State) ->
	{{value, Req}, Q1} = queue:out(Q),
	{req, Ref, From, _Timer, _} = Req,
	gen_fsm:reply(From, {error, timeout}),
    {next_state, connected, State#state{queue = Q1}};

handle_info({timeout, req, _Ref}, StateName, State) ->
	%ignore??
    {next_state, StateName, State};
	
handle_info({tcp, _Socket, Data}, connected, #state{reply = Lines, queue = Q} = State) ->
    case Data of
    <<"ERROR:", Reason/binary>> -> 
		{{value, Req}, Q1} = queue:out(Q),
		{req, _Ref, From, Timer, _} = Req,
		erlang:cancel_timer(Timer),
		gen_fsm:reply(From, {error, binary_to_list(Reason)}),
		{next_state, connected, State#state{queue = Q1}};
    <<"TIME:", _Fields/binary>> = Line -> 
		{next_state, connected, State#state{reply = [Line]}};
	<<"END\r\n">> ->
		{{value, Req}, Q1} = queue:out(Q),
		{req, _Ref, From, Timer, _} = Req,
		erlang:cancel_timer(Timer),
		gen_fsm:reply(From, {ok, lists:reverse(Lines)}),
		{next_state, connected, State#state{reply = [], queue = Q1}};
	Line ->
		{next_state, connected, State#state{reply = [Line|Lines]}}
    end;

handle_info({tcp_closed, Socket}, _StateName, 
	#state{socket = Socket, queue = Q} = State) ->
    ?ERROR("tcp close: ~p", [Socket]),
	lists:foreach(fun({req, _Ref, From, Timer, _}) -> 
		erlang:cancel_timer(Timer),
		gen_fsm:reply(From, {error, tcp_closed})
	end, queue:to_list(Q)),
    retry_connect(),
    {next_state, connecting, State#state{socket = undefined}};

handle_info({tcp_closed, Socket}, StateName, State) ->
    ?ERROR("old tcp socket closed: ~p", [Socket]),
    {next_state, StateName, State};

handle_info({timeout, retry_connect}, connecting, S) ->
    connect(S);

handle_info(Info, StateName, State) ->
    ?ERROR("badinfo: ~p, state: ~p ", [Info, StateName]),
    {next_state, StateName, State}.

handle_event(Event, StateName, State) ->
    ?ERROR("badevent: ~p, stateName: ~p", [Event, StateName]),
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName, State) ->
    Statistics = [{N, get(N)} || N <- [inserted]],
    {reply, {StateName, Statistics}, StateName, State};

handle_sync_event(stop, _From, _StateName, State) ->
    {stop, normal, ok, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

retry_connect() ->
    erlang:send_after(30000, self(), {timeout, retry_connect}).

send_req(Cmd, From, #state{socket = Socket, queue = Q} = State) ->
	gen_tcp:send(Socket, Cmd),
	Req = newreq(From, Cmd),
	Q1 = queue:in(Req, Q),
	{next_state, connected, State#state{queue = Q1}}.

newreq(From, Cmd) ->
	Ref = make_ref(),
	Timer = erlang:send_after(10000, self(), {timeout, req, Ref}),
	{req, Ref, From, Timer, Cmd}.

encode(Metrics) ->
	Tokens = [concat([str(N), "=", strnum(V)]) || {N, V} <- Metrics],
	string:join(Tokens, ",").

