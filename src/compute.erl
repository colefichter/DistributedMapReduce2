-module(compute).

-compile([export_all]).

% Sample map-reduce algorithms --------------------------------
count() ->
    Map = fun(_X) -> 1  end,    
    Reduce = fun lists:sum/1,
    mrs:mapreduce(Map, Reduce).

sum() -> %Find the sum of all the integers stored in the system
    Map = fun(X) -> X end,
    Reduce = fun lists:sum/1,
    mrs:mapreduce(Map, Reduce).

min() -> %Find the smallest integer stored in the system
    Map = fun(X) -> X end,
    Reduce = fun lists:min/1,
    mrs:mapreduce(Map, Reduce).

max() -> %Find the largest integer stored in the system 
    Map = fun(X) -> X end,
    Reduce = fun lists:max/1,
    mrs:mapreduce(Map, Reduce).

mean() ->
    Map = fun(X) -> {X, 1} end,
    Reduce = fun(List) ->
		     {Sum, Count} = lists:foldl(fun({ItemSum, ItemCount}, {TotalSum, TotalCount}) ->
							{TotalSum + ItemSum, TotalCount + ItemCount}	
						end, {0,0}, List),
		     {Sum, Count}
	     end,
	Finalize = fun({Sum, Count}) -> Sum / Count end,
    mrs:mapreduce(Map, Reduce, Finalize).


%% TODO: the following functions need to be rewritten for the fully-distributed Map/Reduce/Finalize style of computing.

% median() ->		  
%     Map = fun(X) -> X end,
%     Reduce = fun(List) ->
% 		     List1 = lists:sort(List),
% 		     N = length(List1),
% 		     I = round(N / 2),
% 		     Median = case (N rem 2) of 
% 				  1 ->
% 				      lists:nth(I, List1);
% 				  0 ->
% 				      Sum = lists:nth(I, List1) + lists:nth(I+1, List1),
% 				      Sum / 2
% 			      end,
% 		     Median
% 	     end,
%      mrs:mapreduce(Map, Reduce).

% mode() -> %Find the most common integer stored in the system, and the number of times it occurs.
%     Map = fun(X) -> {X, 1} end,
%     Reduce = fun(List) -> 
% 		     KeyValuePairs = lists:foldl(fun({X, Count}, Dict) -> dict:update_counter(X, Count, Dict) end, dict:new(), List),
% 		     {MostCommon, MaxCount} = dict:fold(fun(K,V,{MostCommon, MaxCount}) ->
% 					     case V > MaxCount of
% 						 true ->
% 						     {K, V};
% 						 false ->
% 						     {MostCommon, MaxCount}
% 					     end
% 				     end,
% 				     {-1, -99999999},
% 				     KeyValuePairs),
% 		     %[{most_common, MostCommon}, {count, MaxCount}]
% 		     {MostCommon, MaxCount}
% 	     end,
% 	Finalize = fun({MostCommon, MaxCount}) -> [{most_common, MostCommon}, {count, MaxCount}] end,
%     mrs:mapreduce(Map, Reduce, Finalize).
