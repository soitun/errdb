-module(errdb_test).

-export([test_insert/2, test_fetch/1, test_store/2]).

test_insert(I, J) ->
    {T, _} = timer:tc(fun run/2, [I, J]),
    io:format("time: ~p(ms)~n", [T div 1000]).

test_fetch(I) ->
    {T, _} = timer:tc(fun read/1, [I]),
    io:format("time: ~p(ms)~n", [T div 1000]).

test_store(I, J) ->
    {T, _} = timer:tc(fun store/2, [I, J]),
    io:format("time: ~p(ms)~n", [T div 1000]).

run(0, _) ->
    ok;

run(I, J) ->
    timer:sleep(2),
    Key = list_to_binary(["key", i2s(I)]),
    Vals = <<"load1=1,load5=5,load15=15">>,
    Ts = extbif:timestamp(),
    [errdb:insert(Key, Ts+X, Vals) || X <- lists:seq(1, J)],
    run(I - 1, J).

read(0) ->
    ok;
read(I) ->
    timer:sleep(2),
    Key = list_to_binary(["key", i2s(I)]),
    End = extbif:timestamp(),
    Begin = End - 3600,
    errdb:fetch(Key, Begin, End),
    read(I - 1).

store(0, _) ->
    ok;
store(I, J) ->
    Key = list_to_binary([<<"key">>, integer_to_list(I)]),
    Data = [{X, <<"load1=5,load15=6,load5=10">>} || X <- lists:seq(1, J)], 
    errdb_store:write(Key, Data),
    store(I - 1, J).


i2s(I) ->
    integer_to_list(I).
   

