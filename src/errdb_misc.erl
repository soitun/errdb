-module(errdb_misc).

-export([a2b/1, a2l/1,
        b2i/1, b2l/1,
        i2b/1, i2l/1,
        l2a/1, l2b/1,
        pinfo/1,
        number/1,
        line/1,
        dropdot/1,
        version_compare/2,
        version_compare/3]).
a2b(A) ->
    list_to_binary(atom_to_list(A)).

a2l(A) ->
    atom_to_list(A).

b2i(B) when is_binary(B) ->
    list_to_integer(binary_to_list(B)).

b2l(B) when is_binary(B) ->
    binary_to_list(B).

i2b(I) when is_integer(I) ->
    list_to_binary(integer_to_list(I)).

i2l(I) ->
    integer_to_list(I).

l2a(L) ->
    list_to_atom(L).

l2b(L) when is_list(L) ->
    list_to_binary(L).

number(Bin) when is_binary(Bin) ->
    number(binary_to_list(Bin));

number(L) when is_list(L) ->
    case string:to_float(L) of
        {error,no_float} -> list_to_integer(L);
        {F,_Rest} -> F 
    end.

line({Time, Values}) when is_integer(Time) and is_list(Values) ->
    Line = string:join([extbif:to_list(V) || V <- Values], ","),
    string:join([extbif:to_list(Time), Line], ":").

version_compare(A, B, lte) ->
    case version_compare(A, B) of
        eq -> true;
        lt -> true;
        gt -> false
    end;
version_compare(A, B, gte) ->
    case version_compare(A, B) of
        eq -> true;
        gt -> true;
        lt -> false
    end;
version_compare(A, B, Result) ->
    Result =:= version_compare(A, B).

version_compare(A, A) ->
    eq;
version_compare([], [$0 | B]) ->
    version_compare([], dropdot(B));
version_compare([], _) ->
    lt; %% 2.3 < 2.3.1
version_compare([$0 | A], []) ->
    version_compare(dropdot(A), []);
version_compare(_, []) ->
    gt; %% 2.3.1 > 2.3
version_compare(A,  B) ->
    {AStr, ATl} = lists:splitwith(fun (X) -> X =/= $. end, A),
    {BStr, BTl} = lists:splitwith(fun (X) -> X =/= $. end, B),
    ANum = list_to_integer(AStr),
    BNum = list_to_integer(BStr),
    if ANum =:= BNum -> version_compare(dropdot(ATl), dropdot(BTl));
       ANum < BNum   -> lt;
       ANum > BNum   -> gt
    end.

dropdot(A) -> lists:dropwhile(fun (X) -> X =:= $. end, A).

pinfo(Pid) ->
    Props = [registered_name, message_queue_len, 
        total_heap_size, heap_size, reductions],
    Info = process_info(Pid, Props),
    Name = proplists:get_value(registered_name, Info),
    {Name, Info}.

