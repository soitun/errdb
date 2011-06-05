-module(errdb_test).

-export([test_chash/1, test_pg/1, test_insert/2, test_fetch/1, test_store/2]).

test_chash(I) ->
    {Total, _} = timer:tc(fun chash/1, [I]),
    io:format("total: ~p(ms), speed: ~p(/ms)~n", 
        [Total div 1000, (I * 1000 div Total)]).

test_pg(I) ->
    {Total, _} = timer:tc(fun pg/1, [I]),
    io:format("total: ~p(ms), speed: ~p(/ms)~n", 
        [Total div 1000, (I * 1000 div Total)]).

chash(0) ->
    ok;
chash(I) ->
    chash:hash("key" ++ integer_to_list(I)),
    chash(I -1).
    
pg(0) ->
    ok;
pg(I) ->
    chash_pg:get_pid(errdb, "key" ++ integer_to_list(I)),
    pg(I - 1).

test_insert(I, J) ->
    {Total, _} = timer:tc(fun run/2, [I, J]),
    io:format("total: ~p(ms), speed: ~p(/ms)~n", 
        [Total div 1000, (I * J * 1000 div Total)]).

test_fetch(I) ->
    {Total, _} = timer:tc(fun read/1, [I]),
    io:format("time: ~p(ms), speed: ~p(/ms)~n", 
        [Total div 1000, (I * 1000 div Total)]).

test_store(I, J) ->
    {Total, _} = timer:tc(fun store/2, [I, J]),
    io:format("time: ~p(ms), speed: ~p(/ms)~n", 
        [Total div 1000, (I * J * 1000 div Total)]).

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
    timer:sleep(2),
    Key = list_to_binary([<<"key">>, integer_to_list(I)]),
    Data = [{X, <<"load1=5,load15=6,load5=10">>} || X <- lists:seq(1, J)], 
    errdb_store:write(Key, Data),
    store(I - 1, J).

i2s(I) ->
    integer_to_list(I).
   

