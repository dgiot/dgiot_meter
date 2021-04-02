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

-module(dgiot_meter_device).
-author("johnliu").
-include_lib("shuwa_framework/include/shuwa_socket.hrl").
%% API
-export([init/1, handle_info/2, terminate/2]).
-export([start_connect/1]).
-include("dgiot_meter.hrl").
-include_lib("shuwa_framework/include/shuwa_framework.hrl").
-define(MAX_BUFF_SIZE, 10 * 1024).
-record(state, {
    productid,
    devaddr,
    name = <<>>,
    auth = <<>>
}).

start_connect(_Opts =
    #{
        <<"max_reconnect">> := Recon,
        <<"reconnect_times">> := ReTimes,
        <<"port">> := Port,
        <<"ip">> := Ip,
        <<"productid">> := ProductId,
        <<"name">> := Name,
        <<"auth">> := Auth,
        <<"devaddr">> := DevAddr
    }) ->
    State = #state{
        productid = ProductId,
        devaddr = DevAddr,
        name = Name,
        auth = Auth
    },
    shuwa_tcp_client:start_link(?MODULE, Ip, Port, Recon, ReTimes, State).

init(TCPState) ->
    {ok, TCPState}.

handle_info(connection_ready, #tcp{state = #state{devaddr = DevAddr}} = TCPState) ->
    {noreply, TCPState};

handle_info(tcp_closed, TCPState) ->
    {noreply, TCPState};

handle_info({tcp, Buff}, TCPState) when Buff == <<"he">>; Buff == <<"heart">> ->
    shuwa_tcp_client:send(TCPState, <<"ok">>),
    {noreply, TCPState};

%% SKZN,YXZK00010A0405DB,77573902,a,null,null
handle_info({tcp, Buff}, #tcp{state = #state{name = Name, auth = Auth,devaddr = DevAddr}} = TCPState) when Buff == <<"login">> ->
    BinAuth = shuwa_utils:to_binary(Auth),
    {ok, Newname} = iconverl:conv("gbk", "utf8", Name),
    Payload = <<"68", ",", Newname/binary, ",", BinAuth/binary, ",crc">>,
    shuwa_tcp_client:send(TCPState, Payload),
    {noreply, TCPState};

handle_info({tcp, _Buff}, TCPState) ->
%%    lager:info("Buff ~p", [Buff]),
    {noreply, TCPState};

handle_info(_Info, TCPState) ->
    {noreply, TCPState}.

terminate(_Reason, _TCPState) ->
    ok.