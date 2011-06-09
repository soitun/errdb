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

-include("elog.hrl").

-behavior(gen_fsm).

-export([start_link/1, 
        status/1, 
        last/2,
        fetch/4,
        insert/4,
        stop/1]).

%% gen_fsm callbacks
-export([init/1,
     code_change/4,
     handle_info/3,
     handle_event/3,
     handle_sync_event/4,
     terminate/3]).

%% fsm state
-export([connecting/2,
        connecting/3,
        connected/2]).

-define(TCP_OPTIONS, [binary, 
    {packet, line}, 
    {active, true}, 
    {reuseaddr, true}, 
    {send_timeout, 3000}]).

-define(TIMEOUT, 3000).

-record(state, {host, port, socket, queue}).

-import(extbif, [to_list/1, to_binary/1, to_integer/1]).

start_link(Args) ->
    gen_fsm:start_link(?MODULE, [Args], []).

last(_Pid, Key) when is_binary(Key) ->
    {error, unsupport}.

fetch(_Pid, Key, Begin, End) when is_binary(Key) 
    and is_integer(Begin) and is_integer(End) ->
    {error, unsupport}.

insert(Pid, Key, Time, Data) when is_binary(Key)
    and is_integer(Time) and is_binary(Data) ->
    gen_fsm:send_event(Pid, {insert, Key, Time, Data}).

status(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, status).

stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop).

init([Args]) ->
    put(insert, 0),
    put(datetime, {date(), time()}),
    Host = proplists:get_value(host, Args, "localhost"),
    Port = proplists:get_value(port, Args, 7272),
    State = #state{host = Host, port = Port, queue = queue:new()},
    {ok, connecting, State, 0}.

connecting(timeout, State) ->
    connect(State);

connecting(_Event, State) ->
    {next_state, connecting, State}.

connect(#state{host = Host, port = Port} = State) ->
    case gen_tcp:connect(Host, Port, ?TCP_OPTIONS, ?TIMEOUT) of
    {ok, Socket} ->
        {next_state, connected, State#state{socket = Socket}};
    {error, Reason} ->
        ?ERROR("tcp connect failure: ~p", [Reason]),
        retry_connect(),
        {next_state, connecting, State#state{socket = undefined}}
    end.

connecting(Event, _From, S) ->
    ?ERROR("badevent when connecting: ~p", [Event]),
    {reply, {error, connecting}, connecting, S}.

connected({insert, Key, Time, Data}, #state{socket = Socket} = State) ->
    Insert = list_to_binary(["insert ", Key, " ", 
        integer_to_list(Time), " ", Data, "\r\n"]),
    gen_tcp:send(Socket, Insert),
    put(inserted, get(insert)+1),
    {next_state, connected, State}.

handle_info({tcp, _Socket, Data}, connected, State) ->
    ?INFO("tcp data: ~p", [Data]),
    case Data of
    <<"OK", _/binary>> -> %ok reply
        ok; %TODO
    <<"ERROR:", _Reason/binary>> -> %error reply
        ok; %TODO
    _ -> %data reply
        ok %TODO
    end,
    {next_state, connected, State};

handle_info({tcp_closed, Socket}, _StateName, #state{socket = Socket} = State) ->
    ?ERROR("tcp close: ~p", [Socket]),
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

%%%%%%%%%%%%interface%%%%%%%%%%%%%
retry_connect() ->
    erlang:send_after(30000, self(), {timeout, retry_connect}).

