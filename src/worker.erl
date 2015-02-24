-module(worker).

-compile([export_all]).

new() ->
    Pid = spawn(?MODULE, server_loop, [[]]),
    mrs:register_worker(Pid),
    Pid.

server_loop(Numbers) ->    
    receive
	{map, From, Fun} ->
	    io:format("Mapping (~p)~n", [self()]),
	    %entries in this list have the format: {Key, Value}
	    ResultList = lists:map(Fun, Numbers),
	    From ! {map_result, self(), ResultList},
	    server_loop(Numbers);
	{store, Int} ->
	    %io:format("Storing ~p~n", [Int]),
	    server_loop([Int|Numbers]);
        {rebalance, From, NumWorkers, ExpectedIndex} ->
    	    DataToKeep = lists:filter(fun(X) -> ExpectedIndex =:= (X rem NumWorkers) end, Numbers),
    	    DataToPurge = lists:filter(fun(X) -> ExpectedIndex =/= (X rem NumWorkers) end, Numbers),
    	    From ! {purged_data, DataToPurge},
    	    server_loop(DataToKeep);
	{reset} ->
	    server_loop([]);
	{print} ->
	    io:format(" ~p: ~w~n", [self(), Numbers]),
	    server_loop(Numbers)
    end.
