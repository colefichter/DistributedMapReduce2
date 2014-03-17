-module(bootstrap).

-compile([export_all]).

start() ->
    io:format(" Starting...~n"),
    application:load(resource_discovery),
    application:start(resource_discovery),    
    ok = ensure_contact(),    
    mrs:start(),
    worker:new(),
    worker:new(),
    worker:new(),
    seed().    

seed(N) ->
    Integers = lists:seq(0, N),
    lists:foreach(fun (I) ->
			  mrs:store(I)
		  end, Integers).    
seed() ->
    seed(99).



ensure_contact() ->
    % DefaultNodes = ['contact1@localhost', 'contact2@localhost'],
    % case get_env(simple_cache, contact_nodes, DefaultNodes) of
    %     [] ->
    %         {error, no_contact_nodes};
    %     ContactNodes ->
    %         ensure_contact(ContactNodes)
    % end.
    io:format(" Joining cluster...~n"),
    ensure_contact(['contact1@localhost', 'contact2@localhost']).

ensure_contact(ContactNodes) ->
    io:format(" Contacting other nodes...~n"),
    Answering = [N || N <- ContactNodes, net_adm:ping(N) =:= pong],
    case Answering of
        [] ->
            {error, no_contact_nodes_reachable};
        _ ->
            WaitTime = 1000,
            wait_for_nodes(length(Answering), WaitTime)
    end.

wait_for_nodes(MinNodes, WaitTime) ->
    io:format(" Waiting for other nodes to respond...~n"),
    Slices = 10,
    SliceTime = round(WaitTime/Slices),
    wait_for_nodes(MinNodes, SliceTime, Slices).

wait_for_nodes(_MinNodes, _SliceTime, 0) ->
    ok;
wait_for_nodes(MinNodes, SliceTime, Iterations) ->    
    case length(nodes()) > MinNodes of
        true ->
          ok;
        false ->
            timer:sleep(SliceTime),
            wait_for_nodes(MinNodes, SliceTime, Iterations - 1)
    end.

% get_env(AppName, Key, Default) ->
%     case application:get_env(AppName, Key) of
%         undefined   -> Default;
%         {ok, Value} -> Value
%     end.