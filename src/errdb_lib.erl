-module(errdb_lib).

%rows <-> columns transform
-export([transform/1]).

-export([decode/1, encode/1, encode/2, line/2]).

-export([binary/1, number/1, str/1, strnum/1]).

transform(XData) ->
	YData = 
	lists:foldl(fun({X, XList}, Dict) -> 
		lists:foldl(fun({Y, V}, Dict0) -> 
			case dict:find(Y, Dict0) of
			{ok, YList} ->
				dict:store(Y, [{X, V}|YList], Dict0); 
			error -> 
				dict:store(Y, [{X, V}], Dict0)
			end
		end, Dict, XList)
	end, dict:new(), XData),
	dict:to_list(YData).

%Data: "k=v,k1=v1,k2=v2"
%return: [{"k",v}, {"k1", v1}]
decode(S) when is_list(S) ->
	Metrics = [ list_to_tuple(string:tokens(Token, "=")) 
		|| Token <- string:tokens(S, ",") ],
	[{Field, number(Value)} || {Field, Value} <- Metrics].

encode(Values) ->
	string:join([str(V) || V <- Values], ",").

encode(Fields, Rows) ->
	Head = string:join([binary_to_list(F) || F <- Fields], ","),
	Lines = string:join([encode(Row) || Row <- Rows], "\r\n"),
	list_to_binary([Head, "\r\n", Lines, "\r\nEND\r\n"]).

line(Time, Values) ->
	Line = string:join([format(V) || V <- Values], ","),
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

format(undefined) ->
	"";
format(V) ->
	str(V).

str(V) when is_integer(V) ->
    integer_to_list(V);
str(V) when is_float(V) ->
    [S] = io_lib:format("~.6f", [V]), S;
str(V) when is_atom(V) ->
	atom_to_list(V);
str(V) when is_list(V) ->
	V;
str(V) when is_binary(V) ->
	binary_to_list(V).

strnum(V) when is_integer(V) ->
    integer_to_list(V);
strnum(V) when is_float(V) ->
    [S] = io_lib:format("~.6f", [V]), S;
strnum(_) ->
	"0".

