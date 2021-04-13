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
-author("johnliu").
-include_lib("shuwa_framework/include/shuwa_socket.hrl").
-include("dgiot_meter.hrl").

%% API
-export([start/2]).

%% TCP callback
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, terminate/2, code_change/3]).

start(Port, State) ->
    shuwa_tcp_server:child_spec(?MODULE, shuwa_utils:to_int(Port), State).

%% =======================
%% tcp server start
%% {ok, State} | {stop, Reason}
init(TCPState) ->
    shuwa_metrics:inc(dgiot_meter, <<"dtu_login">>, 1),
    {ok, TCPState}.

%%设备登录报文
handle_info({tcp, DtuAddr}, #tcp{socket = Socket, state = #state{id = ChannelId, dtuaddr = <<>>} = State} = TCPState)
    when byte_size(DtuAddr) == 15 ->
    lager:info("DevAddr ~p ChannelId ~p", [DtuAddr, ChannelId]),
    DTUIP = shuwa_evidence:get_ip(Socket),
    dgiot_meter:create_dtu(DtuAddr, ChannelId, DTUIP),
    erlang:send_after(1000, self(), search_meter),
    {noreply, TCPState#tcp{buff = <<>>, state = State#state{dtuaddr = DtuAddr, step = search_meter}}};

handle_info({tcp, ErrorBuff}, #tcp{state = #state{dtuaddr = <<>>} = State} = TCPState) ->
    lager:info("ErrorBuff ~p ", [ErrorBuff]),
    {noreply, TCPState#tcp{buff = <<>>, state = State#state{step = search_meter}}};

%%设备指令返回报文
handle_info({tcp, Buff}, #tcp{socket = Socket,
    state = #state{id = ChannelId, dtuaddr = DtuAddr, step = search_meter} = State} = TCPState) ->
    lager:info("Buff ~p", [shuwa_utils:binary_to_hex(Buff)]),
    [#{<<"addr">> := Addr} | _] = Frames = shuwa_smartmeter:parse_frame(dlt645, Buff, []),
    lager:info("Frames ~p", [Frames]),
    DTUIP = shuwa_evidence:get_ip(Socket),
    dgiot_meter:create_meter(shuwa_utils:binary_to_hex(Addr), ChannelId, DTUIP, DtuAddr),
    {noreply, TCPState#tcp{buff = <<>>, state = State#state{step = shuwa_smartmeter:search_meter(tcp, TCPState,1)}}};

handle_info({tcp, Buff}, #tcp{state = #state{id = ChannelId, step = read_meter}} = TCPState) ->
    [#{<<"addr">> := Addr, <<"value">> := Value} | _] = shuwa_smartmeter:parse_frame(dlt645, Buff, []),
    {ProductId, _ACL, _Properties} = shuwa_data:get({meter, ChannelId}),
    DevAddr = shuwa_utils:binary_to_hex(Addr),
    Topic = <<"thing/", ProductId/binary, "/", DevAddr/binary, "/post">>,
    shuwa_mqtt:publish(DevAddr, Topic, jsx:encode(Value)),
    {noreply, TCPState#tcp{buff = <<>>}};

handle_info(search_meter, #tcp{state = State} = TCPState) ->
    {noreply, TCPState#tcp{buff = <<>>, state = State#state{step = shuwa_smartmeter:search_meter(tcp, TCPState,1)}}};

%%API发消息
handle_info({deliver, _Topic, Msg}, #tcp{state = #state{id = ChannelId, step = read_meter}} = TCPState) ->
    case binary:split(shuwa_mqtt:get_topic(Msg), <<$/>>, [global, trim]) of
        [<<"thing">>, ProductId, DevAddr] ->
            #{<<"thingdata">> := ThingData} = jsx:decode(shuwa_mqtt:get_payload(Msg), [{labels, binary}, return_maps]),
            lager:info("ThingData ~p ProductId ~p , DevAddr ~p", [ThingData, ProductId, DevAddr]),
            Payload = shuwa_smartmeter:to_frame(ThingData),
            lager:info("Payload ~p", [shuwa_utils:binary_to_hex(Payload)]),
            shuwa_tcp_server:send(TCPState, Payload),
            shuwa_bridge:send_log(ChannelId, "from_task: ~ts:  ~ts ", [_Topic, unicode:characters_to_list(shuwa_mqtt:get_payload(Msg))]);
        _ ->
            pass
    end,
    {noreply, TCPState};

handle_info({deliver, _Topic, Msg}, TCPState) ->
    lager:info("Payload ~p", [shuwa_mqtt:get_payload(Msg)]),
    {noreply, TCPState};

%% {stop, TCPState} | {stop, Reason} | {ok, TCPState} | ok | stop
handle_info(_Info, TCPState) ->
    {noreply, TCPState}.

handle_call(_Msg, _From, TCPState) ->
    {reply, ok, TCPState}.

handle_cast(_Msg, TCPState) ->
    {noreply, TCPState}.

terminate(_Reason, _TCPState) ->
    shuwa_metrics:dec(dgiot_meter, <<"dtu_login">>, 1),
    ok.

code_change(_OldVsn, TCPState, _Extra) ->
    {ok, TCPState}.

