-module(mrs).
%MRS - a MapReduce Server.

-compile([export_all]).

-define(SERVER, mrs).
-define(WAIT_FOR_RESOURCES, 1000).

%client API ----------------------------------------------------
register_worker(Pid) ->
    ?SERVER ! {register_worker, Pid}.

store(Integers) when is_list(Integers) ->
    lists:foreach(fun(X) -> ?SERVER ! {store, X} end, Integers);	
store(Int) ->
    ?SERVER ! {store, Int}.

print() ->
    message_cluster({print}),
    ok.

reset() ->
    message_cluster({reset}),
    ok.

mapreduce(Map, Reduce) -> 
    mapreduce(Map, Reduce, fun(X) -> X end),
    ok.

mapreduce(Map, Reduce, Finalize) ->
    N = message_cluster({mapreduce, self(), Map, Reduce}),
    Values = collect_reduce_replies(N),
    io:format("  ReduceReplies: ~p~n", [Values]),
    ReduceResult = Reduce(Values),
    io:format("  Last Reduce Result: ~p~n", [ReduceResult]),
    FinalResult = Finalize(ReduceResult),
    io:format("Final Result of Distributed MapReduce Job: ~p~n", [FinalResult]).

rebalance() ->
	?SERVER ! {rebalance}.

%server implementation ----------------------------------------
start() ->
    io:format(" Spawning MRS server...~n"),
    Workers = [],
    Pid = spawn(?MODULE, server_loop, [Workers]),
    register(?SERVER, Pid),
    resource_discovery:add_local_resource(?SERVER, Pid),
    resource_discovery:add_target_resource_type(?SERVER),
    resource_discovery:trade_resources(),
    io:format(" Waiting for resource discovery...~n"),
    timer:sleep(?WAIT_FOR_RESOURCES),
    io:format(" Finished waiting for resource discovery.~n").

server_loop(Workers) -> % The main processing loop for the server.
    receive
	{mapreduce, From, MapFun, ReduceFun} ->
	    Self = self(),
	    lists:foreach(fun (Pid) ->
				  Pid ! {map, Self, MapFun}
			  end, Workers),
	    N = length(Workers),	    
	    %% Wait for N Map processes to terminate
	    MapResults1 = collect_map_replies(N),
	    %Group results into a single list
	    MapResults2 = dict:fold(fun (_Key, Value, Acc0) -> [Value|Acc0] end, [], MapResults1),
	    ReduceResult = ReduceFun(lists:flatten(MapResults2)),
	    io:format("Sending MapReduce Result: ~p~n", [ReduceResult]),
	    From ! {reduce_result, self(), ReduceResult},
	    server_loop(Workers);	    
	{store, Int} ->
	    %For the hash, we'll just find n MOD num_workers and store on that machine.
	    Index = (Int rem length(Workers)) + 1, %lists use 1-based indexing
	    Worker = lists:nth(Index, Workers),
	    Worker ! {store, Int},
	    server_loop(Workers);	  
	{rebalance} ->
	    io:format("Rebalancing Data...~n"),
	    NumWorkers = length(Workers),
	    From = self(),
	    _unused = lists:foldl(fun(Worker, Index) ->
				Worker ! {rebalance, From, NumWorkers, Index},
				receive
					{purged_data, Items} ->
						lists:foreach(fun mrs:store/1, Items)
				end,
				Index + 1
			end, 0, Workers),
	    server_loop(Workers);
	{print} ->
	    io:format("Workers: ~p~n", [Workers]),
	    lists:foreach(fun (Pid) ->  Pid ! {print} end, Workers),
	    server_loop(Workers);
	{register_worker, Pid} ->
	    Id = length(Workers) + 1,
	    io:format("Registering worker ~p (~p).~n", [Id, Pid]),
	    mrs:rebalance(),	    
	    server_loop([Pid|Workers]);
	{reset} ->
	    lists:foreach(fun(Pid) -> Pid ! {reset} end, Workers),
	    server_loop(Workers)
    end.

collect_map_replies(N) ->
    collect_map_replies(N, dict:new()).

collect_map_replies(0, Dict) ->
    Dict;
collect_map_replies(N, Dict) ->
    receive
	{map_result, Worker, ResultList} ->	   
	    case dict:is_key(Worker, Dict) of
		true ->
		    io:format("THIS SHOULDN'T HAPPEN!", []);		    
		false ->
		    Dict1 = dict:store(Worker, ResultList, Dict),
		    collect_map_replies(N-1, Dict1)
	    end
    end.

collect_reduce_replies(N) ->
    collect_reduce_replies(N, dict:new()).

collect_reduce_replies(0, Dict) ->    
    Values = [V || {_K, V} <- dict:to_list(Dict)],
    Values;
collect_reduce_replies(N, Dict) -> %BUG: if a node leaves the cluster, this will hang! Should be using a cast.
    receive
		{reduce_result, FromServer, ReduceResult} ->	   
		    case dict:is_key(FromServer, Dict) of
			true ->
			    io:format("THIS SHOULDN'T HAPPEN!", []);		    
			false ->
			    Dict1 = dict:store(FromServer, ReduceResult, Dict),
			    collect_reduce_replies(N-1, Dict1)
		    end
	after
		1000 -> %Timeout... one or more nodes has left the cluster!			
			collect_reduce_replies(0, Dict)
    end.

message_cluster(MessageTuple) ->
    {ok, Servers} = resource_discovery:fetch_resources(?SERVER),		    
    lists:foreach(fun(Pid) -> Pid ! MessageTuple end, Servers),
    length(Servers).
