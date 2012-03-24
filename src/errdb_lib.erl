-module(errdb_lib).

-export([decode/1, encode/1, encode/2, line/2]).

-export([binary/1, number/1]).

%Data: k=v,k1=v1,k2=v2
%return: {["k","k1","k2"], [v,v1,v2]}
decode(S) when is_list(S) ->
	Metrics = [ list_to_tuple(string:token(Token, "=")) 
		|| Token <- string:tokens(S, ",") ],
	[{Field, number(Value)} || {Field, Value} <- Metrics].

encode(Values) ->
	string:join([str(V) || V <- Values], ",").

encode(Fields, Rows) ->
	Head = string:join([binary_to_list(F) || F <- Fields], ","),
	Lines = string:join([encode(Row) || Row <- Rows], "\r\n"),
	list_to_binary([Head, "\r\n", Lines, "\r\nEND\r\n"]).

line(Time, Values) ->
	Line = string:join([str(V) || V <- Values], ","),
	lists:concat([integer_to_list(Time), ":", Line]).

binary(A) when is_atom(A) ->
	binary(atom_to_list(A));

binary(L) when is_list(L) ->
	list_to_binary(L);

binary(B) when is_binary(B) ->
	B.

number(Bin) when is_binary(Bin) ->
    number(binary_to_list(Bin));

number(L) when is_list(L) ->
    case string:to_float(L) of
        {error,no_float} -> list_to_integer(L);
        {F,_Rest} -> F 
    end.

str(V) when is_integer(V) ->
    integer_to_list(V);

str(V) when is_float(V) ->
    [S] = io_lib:format("~.6f", [V]),
    S.

