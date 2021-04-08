%%--------------------------------------------------------------------
%% Copyright (c) 2020 DGIOT Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(dgiot_meter_tcp).
-behavior(shuwa_channelx).
-author("johnliu").
-include_lib("shuwa_framework/include/shuwa_socket.hrl").
-include("dgiot_meter.hrl").

%% API
-export([start/2]).

%% TCP callback
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, terminate/2, code_change/3]).

start(Port,State) ->
    shuwa_tcp_server:child_spec(?MODULE, shuwa_utils:to_int(Port), State).

%% =======================
%% tcp server start
%% {ok, State} | {stop, Reason}
init(#tcp{state = #state{id = ChannelId}} = TCPState) ->
    shuwa_metrics:inc(dgiot_meter,<<"dtu_login">>,1),
    case shuwa_bridge:get_products(ChannelId) of
        {ok, _, _ProductIds} ->
            Delay = shuwa_data:get_consumer(ChannelId, 1),
            erlang:send_after(20 * 1000 + Delay * 100, self(), login),
            {ok, TCPState};
        {error, not_find} ->
            {stop, not_find_channel}
    end.

%%设备登录报文
handle_info({tcp, Buff}, #tcp{socket = Socket, state = #state{id = ChannelId, devaddr = <<>>, productid = <<>>} = State} = TCPState) ->
    lager:info("Buff ~p",[Buff]),
    {noreply, TCPState};

%%设备指令返回报文
handle_info({tcp, Buff}, #tcp{state = #state{id = ChannelId} = State} = TCPState) ->
    lager:info("Buff ~p",[Buff]),
    {noreply, TCPState#tcp{buff = <<>>, state = State#state{id = ChannelId}}};

handle_info(login, #tcp{state = #state{id = ChannelId}} = TCPState) ->
    Payload = <<"afa">>,
    shuwa_tcp_server:send(TCPState, Payload),
    shuwa_metrics:inc(dgiot_meter, <<"dtu_login_ack">>,1),
    case shuwa_data:get({ChannelId, heartbeat}) of
        not_find -> pass;
        {_Heartbeat, Version, _Port} ->
            case Version of
                <<"0.0.1">> ->
                    erlang:send_after(5000, self(), heart);
                _ ->
                    erlang:send_after(5000, self(), he)
            end
    end,
    {noreply, TCPState#tcp{buff = <<>>, state = #state{id = ChannelId}}};


%%API发消息
handle_info({deliver, _Topic, Msg}, #tcp{state = #state{devaddr = DtuAddr, productid = ProductId}} = TCPState) ->
    #{<<"objectId">> := DeviceId} =
        shuwa_parse:get_objectid(<<"Device">>, #{<<"product">> => ProductId, <<"devaddr">> => DtuAddr}),
    Auth =
        case shuwa_data:get({DeviceId, meterdev}) of
            not_find ->
                <<"12345678">>;
            {_, Auth1} ->
                Auth1
        end,
    Payload = binary_to_term(shuwa_mqtt:get_payload(Msg)),
    case Payload of
        #{<<"instruct_id">> := Id, <<"pid">> := Pid, <<"auth">> := HardAuth} ->
            case Auth == HardAuth of
                true ->
                    put(Id, Pid),
                    Frame = dgiot_meter_decoder:to_frame(Payload#{<<"devtype">> => <<"METER">>}),
                    shuwa_tcp_server:send(TCPState, Frame);
                false ->
                    Pid ! {error}
            end;
        _ ->
            pass
    end,
    {noreply, TCPState};

%% {stop, TCPState} | {stop, Reason} | {ok, TCPState} | ok | stop
handle_info(_Info, TCPState) ->
    {noreply, TCPState}.

handle_call(_Msg, _From, TCPState) ->
    {reply, ok, TCPState}.

handle_cast(_Msg, TCPState) ->
    {noreply, TCPState}.

terminate(_Reason, _TCPState) ->
    shuwa_metrics:dec(dgiot_meter,<<"server_login">>,1),
    ok.

code_change(_OldVsn, TCPState, _Extra) ->
    {ok, TCPState}.

init_data(Products, ChannelId, Heartbeat, Version, Port, Total) ->
    dgiot_meter:get_Dict(),
    dgiot_meter:get_Product(),
    lists:map(fun({ProductId, _}) ->
        case shuwa_parse:get_object(<<"Product">>, ProductId) of
            {ok, #{<<"name">> := Name, <<"devType">> := DevType, <<"ACL">> := Acl}} ->
                NewRole = lists:foldl(fun(X, Acc) ->
                    case X of
                        <<"*">> -> Acc;
                        _ ->
                            <<"role:", Role/binary>> = X,
                            Role
                    end
                                      end, <<>>, maps:keys(Acl)),
                shuwa_data:insert({meter, Name}, {ProductId, DevType, NewRole}),
                shuwa_data:insert({ChannelId, Name}, ProductId);
            _ -> pass
        end
              end, Products),
    shuwa_data:insert({ChannelId, heartbeat}, {Heartbeat, Version, Port}),
    Fun = fun(Page) ->
        lists:map(fun(X) ->
               do_something(X)
                  end, Page)
          end,
    Query = #{<<"where">> => #{}, <<"keys">> => [<<"objectId">>, <<"basedata">>]},
    shuwa_parse_loader:start(<<"Device">>, Query, 0, 100, Total, Fun).

start_timer(Time, Fun) ->
    spawn(fun() ->
        timer(Time, Fun)
          end).

timer(Time, Fun) ->
    receive
        cancel ->
            void
    after Time ->
        Fun()
    end.

do_something(Device) ->
    lager:info("~p",[Device]).