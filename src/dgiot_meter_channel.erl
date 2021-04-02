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

%%#  用户场景
%%从用户到设备，主要需要解决如下几个问题：
%%+ 人与设备的关系，User基于流动性，权限系统里一般不会直接绑定User与Device的权限关系，中间会通过Department(Role)来间接绑定ACL
%%+ 设备与设备的关系，设备与设备之间有可能存在真实的关系，例如DTU与Meter，也可能只有一种虚拟关系，例如Group与DTU，属于因工程需要，临时拉群
%%+ 对具体设备来说，1、需要一个UID来表征身份；2、需要一个route来表征关系；3、需要多个tag来表征特征
%%+ 重点讨论Meter、DTU和TCP Server的交互过程
%%
%%| No.|名称|   Meter         |   DTU                  | TCP Server                 |  说明      |
%%| --| ----   | -------      | ------                 | -----------               |-----------|
%%|1 |连接     |               | send ->  [IP]           | ack <-- [IP]             | 必选      |
%%|2 |登陆     |               | send ->  [DtuAddr]      | ack <-- [DtuAddr]        | 可选，可用IP代替|
%%|3 |扫串口   | ack-> [485]   | send <-- [search 485]   | send <--[search 485]    | 可选，有档案可免 |
%%|4 |扫modbus | ack-> [modbus]| send <-- [search modbus]   | send <--[search modbus] |可选，有档案可免 |
%%|5 |扫表 | ack-> [Meter Addr]| send <-- [search meter]   | send <--[search meter] |可选，有档案可免 |
%%|6 |抄表 | ack-> [Meter Data]| send <-- [read meter]   | send <--[read meter] |必选 |
%%|7 |写表 | ack-> [Meter Control]| send <-- [write meter]   | send <--[write meter] |可选 |
%%|8 |登出 |       |  send -> [DtuAddr] |  ack <-- [DtuAddr]        |可选 |
%%|9 |断开 |     |  send -> [IP]      |  do_something        |必选 |
%%
%%扫串口、扫描modbus、扫表是一个费时费流量的操作，例如扫表一般至少需要扫256次，一般只会在物联网工程施工阶段进行，并完成相应的自动注册功能，形成设备档案，正常运行中不进行这些操作。
%%
%%这也是为什么电力抄表非常强调电表档案建设，如果没有档案，每一次DTU掉线都需要重新进行非常耗时耗流量的扫描任务
%%
%%整体交互流程如下
%%
%%```
%%---------------------------------------------------------------------------------------------------------------------
%%|             物理层                                      |   连接层                 |      虚拟层            | 应用层|
%%---------------------------------------------------------------------------------------------------------------------
%%User1(Session)                User2(Session)
%%|                              |
%%API                            API             <--http--> shuwa_rest  --+
%%|                              |                                       |
%%+                              +                                       |
%%Department1(Role)             Department2(Role)                             |
%%|                              |                                       |
%%ACL                            ACL            <--parse--> shuaw_parse --+
%%+                              +                                       |              +-- 时序数据--+
%%Group(Devcie)                 Group(Devcie)                                |              |            |
%%|                              |                                       | === 流计算==> 物理层镜像    +--> 批量计算
%%+--------+-------+                      +                                       |              |            |
%%|                |                      |                                       |              +-- 关系数据--+
%%DTU1(Device1)    DTU2(Device)           DTU3(Device)        <--tcp-->  tcp_server ---+
%%|                |                      |                                       |
%%modbus             modbus                modbus            <--modbus-->  proctol ---+
%%|                |                      |                                       |
%%+                +                      +                                       |
%%485              485                     485             <--485-->    proctol  --+
%%|                |                      |                                       |
%%+                +             +--------+--------+                              |
%%|                |             |                 |                              |
%%Meter1(Device) Meter2(Device) Meter4(Device）Meter5(Device）<--DLT/645--> proctol --+
%%---------------------------------------------------------------------------------------------------------------------
-module(dgiot_meter_channel).
-behavior(shuwa_channelx).
-author("johnliu").
-include_lib("shuwa_framework/include/shuwa_socket.hrl").
-include("dgiot_meter.hrl").
-define(TYPE, <<"METERDTU">>).
-define(MAX_BUFF_SIZE, 1024).
-define(SECS, [5, 5 * 60]).
-define(JIAOSHI, 60 * 10 * 1000).
-record(state, {
    id,
    env = #{},
    buff_size = 1024000,
    heartcount = 0,
    devaddr = <<>>,
    hardware_name = 0,
    productid = <<>>
}).
%% API
-export([start/2]).

%% Channel callback
-export([init/3, handle_init/1, handle_event/3, handle_message/2, stop/3]).
%% TCP callback
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, terminate/2, code_change/3]).


%% 注册通道类型
-channel(?TYPE).
-channel_type(#{
    type => 1,
    title => #{
        zh => <<"METERDTU采集通道"/utf8>>
    },
    description => #{
        zh => <<"METERDTU采集通道"/utf8>>
    }
}).
%% 注册通道参数
-params(#{
    <<"port">> => #{
        order => 1,
        type => integer,
        required => true,
        default => 51888,
        title => #{
            zh => <<"端口"/utf8>>
        },
        description => #{
            zh => <<"侦听端口"/utf8>>
        }
    },
    <<"heartbeat">> => #{
        order => 2,
        type => integer,
        required => true,
        default => 10,
        title => #{
            zh => <<"心跳周期"/utf8>>
        },
        description => #{
            zh => <<"心跳周期"/utf8>>
        }
    },
    <<"version">> => #{
        order => 3,
        type => string,
        required => true,
        default => <<"0.0.1">>,
        title => #{
            zh => <<"DTU版本"/utf8>>
        },
        description => #{
            zh => <<"DTU版本"/utf8>>
        }
    }
}).

start(ChannelId, ChannelArgs) ->
    shuwa_channelx:add(?TYPE, ChannelId, ?MODULE, ChannelArgs).

%% 通道初始化
init(?TYPE, ChannelId, #{
    <<"port">> := Port,
    <<"heartbeat">> := Heartbeat,
    <<"version">> := Version,
    <<"product">> := Products
} = Args) ->
    shuwa_data:set_consumer(ChannelId, 200),
    State = #state{
        id = ChannelId,
        buff_size = maps:get(<<"buff_size">>, Args, 1024000)
    },
    start_timer(5000, fun() -> init_data(Products, ChannelId, Heartbeat, Version, Port, 100000) end),
    {ok, State, shuwa_tcp_server:child_spec(?MODULE, shuwa_utils:to_int(Port), State)};

init(?TYPE, _ChannelId, _Args) ->
    {ok, #{}, #{}}.

handle_init(State) ->
    {ok, State}.

%% 通道消息处理,注意：进程池调用
handle_event(_EventId, _Event, State) ->
    {ok, State}.

handle_message(_Message, State) ->
    {ok, State}.

stop(_ChannelType, _ChannelId, _State) ->
    ok.

%% =======================
%% tcp server start
%% {ok, State} | {stop, Reason}
init(#tcp{state = #state{id = ChannelId}} = TCPState) ->
    shuwa_metrics:inc(dgiot_meter,<<"server_login">>,1),
    case shuwa_bridge:get_products(ChannelId) of
        {ok, ?TYPE, _ProductIds} ->
            Delay = shuwa_data:get_consumer(ChannelId, 1),
            erlang:send_after(20 * 1000 + Delay * 100, self(), login),
            {ok, TCPState};
        {error, not_find} ->
            {stop, not_find_channel}
    end.

%%设备登录报文
handle_info({tcp, Buff}, #tcp{socket = Socket, state = #state{id = ChannelId, devaddr = <<>>, productid = <<>>} = State} = TCPState) ->
    shuwa_metrics:inc(dgiot_meter,<<"server_register">>,1),
    DTUIP = shuwa_evidence:get_ip(Socket),
    {DtuAddr, ProductId1, Hardware_name} =
        case dgiot_meter_decoder:parse_frame(Buff, no) of
            {_Buff1, [#{<<"dtu">> := DtuAddr1, <<"meter">> := _Devaddr1} | _]} when DtuAddr1 =/= <<>> ->
                {Name, DtuAddr2, _} = dgiot_meter:get_name(DtuAddr1),
                {ProductId, DeviceId, DevAddr} = dgiot_meter:get_objectid(DtuAddr2, ChannelId),
                shuwa_mqtt:subscribe(<<"thing/", ProductId/binary, "/", DevAddr/binary>>),
                dgiot_meter:create_device(DeviceId, DTUIP, Name, DevAddr),
                Topic = <<"thing/", ProductId/binary, "/", DevAddr/binary>>,
                Payloadmqtt = <<Name/binary, "+", DevAddr/binary, "+ONLINE">>,
                shuwa_mqtt:publish(self(), Topic, Payloadmqtt),
                {DevAddr, ProductId, Name};
            _ ->
                {<<>>, <<>>, <<>>}
        end,
    shuwa_data:insert({heart, DtuAddr}, self()),
    {noreply, TCPState#tcp{state = State#state{productid = ProductId1, devaddr = DtuAddr, hardware_name = Hardware_name}}};

%%设备指令返回报文
handle_info({tcp, Buff}, #tcp{state = #state{id = ChannelId, heartcount = _Heartcount} = State} = TCPState) ->
    case dgiot_meter_decoder:parse_frame(Buff, State) of
        {_Buff1, [#{<<"dtu">> := DtuAddr1, <<"instruct_id">> := Id} = Msg | _]} when DtuAddr1 =/= <<>> ->
            case get(Id) of
                undefined -> pass;
                Pid ->
                    case dgiot_meter:get_name(DtuAddr1) of
                        {_Name1, _DevAddr1, zh} ->
                            Pid ! {hardware_msg, Msg};
                        {_Name2, _DevAddr2, en} ->
                            Msg1 = maps:without([<<"name">>, <<"role">>], Msg),
                            Pid ! {hardware_msg, Msg1}
                    end
            end;
        _ ->
            pass
    end,
    {noreply, TCPState#tcp{buff = <<>>, state = State#state{id = ChannelId, heartcount = 0}}};

handle_info(login, #tcp{state = #state{id = ChannelId}} = TCPState) ->
    Payload = <<"afa">>,
    shuwa_tcp_server:send(TCPState, Payload),
    shuwa_metrics:inc(dgiot_meter, <<"server_login_ack">>,1),
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


%%速客新DTU采集通道，授权码第一次上线查询一次
handle_info(he, #tcp{state = #state{id = ChannelId, heartcount = Heartcount} = State} = TCPState) ->
    case shuwa_data:get({ChannelId, heartbeat}) of
        not_find ->
            erlang:send_after(180 * 1000, self(), he),
            shuwa_tcp_server:send(TCPState, <<"he">>)
    end,
    {noreply, TCPState#tcp{buff = <<>>, state = State#state{id = ChannelId, heartcount = Heartcount + 1}}};

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