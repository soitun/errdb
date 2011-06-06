-module(errdb_file).

-export([write/3, read/1, line/1]).

write(FileName, Fields, Records) ->
    {ok, File} = file:open(FileName, [read, write, append, raw, {read_ahead, 1024}]), 
    DsCnt = length(Fields),
    DsData = list_to_binary(string:join(Fields, ",")),
    DsSize = size(DsData),
    {LastUp, _} = lists:last(Records),
    LastRow = length(Records) - 1,
    Head = <<"RRDB0001", LastUp:32, LastRow:32, DsCnt:32, DsSize:32, DsData/binary>>,
    Lines = [line(Record) || Record <- Records],
    file:write(File, Head),
    file:pwrite(File, 1024, Lines).

read(FileName) ->
    {ok, File} = file:open(FileName, [read, binary, raw, {read_ahead, 1024}]), 
    {ok, Data} = file:read(File, 1024),
    <<"RRDB0001", LastUp:32, LastRow:32, DsCnt:32, DsSize:32, DsData/binary>> = Data,
    <<DsList:DsSize/binary, _/binary>> = DsData,
    io:format("lastup: ~p~n", [LastUp]),
    io:format("lastrow: ~p~n", [LastRow]),
    io:format("dscnt: ~p~n", [DsCnt]),
    io:format("dssize: ~p~n", [DsSize]),
    io:format("dslist: ~p~n", [DsList]).

line({Time, List}) ->
    Data = [<<E/float>> || E <- List],
    list_to_binary([<<Time:32>>, Data, "\n"]).
