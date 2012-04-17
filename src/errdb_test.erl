-module(errdb_test).

-export([test_chash/1, test_pg/1, test_insert/2, test_fetch/1]).

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

run(0, _) ->
    ok;

run(I, J) ->
    timer:sleep(2),
    Key = "key" ++ integer_to_list(I),
    Vals = [{"load1", 1},{"load5", 5},{"load15", 15}],
    Ts = extbif:timestamp(),
    [errdb:insert(Key, Ts+X-(I*1000), Vals) || X <- lists:seq(1, J)],
    run(I - 1, J).

read(0) ->
    ok;
read(I) ->
    timer:sleep(2),
    Key = "key" ++ integer_to_list(I),
    End = extbif:timestamp(),
    Begin = End - 3600,
    errdb:fetch(Key, ["load1"], Begin, End),
    read(I - 1).
