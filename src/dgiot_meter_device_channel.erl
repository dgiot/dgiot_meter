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

-module(dgiot_meter_device_channel).
-author("johngliu").
-behavior(shuwa_channelx).
-include("dgiot_meter.hrl").
-define(TYPE, <<"METERDEV">>).
-record(state, {id}).
%% API
-export([
    start/2
]).

%% Channel callback
-export([init/3, handle_init/1, handle_event/3, handle_message/2, stop/3]).

-channel(?TYPE).
-channel_type(#{
    type => 1,
    title => #{
        zh => <<"METERDEV资源通道"/utf8>>
    },
    description => #{
        zh => <<"METERDEV资源通道"/utf8>>
    }
}).
%% 注册通道参数
-params(#{
    <<"ip">> => #{
        order => 1,
        type => string,
        required => true,
        default => <<"127.0.0.1"/utf8>>,
        title => #{
            zh => <<"服务器地址"/utf8>>
        },
        description => #{
            zh => <<"服务器地址"/utf8>>
        }
    },
    <<"port">> => #{
        order => 2,
        type => integer,
        required => true,
        default => 51889,
        title => #{
            zh => <<"服务器端口"/utf8>>
        },
        description => #{
            zh => <<"服务器端口"/utf8>>
        }
    },
    <<"buff_size">> => #{
        order => 7,
        type => integer,
        required => false,
        default => 1024000,
        title => #{
            zh => <<"缓存数据量"/utf8>>
        },
        description => #{
            zh => <<"缓存数据量"/utf8>>
        }
    },
    <<"total">> => #{
        order => 8,
        type => integer,
        required => false,
        default => 2000,
        title => #{
            zh => <<"设备数量"/utf8>>
        },
        description => #{
            zh => <<"设备数量"/utf8>>
        }
    }
}).

start(ChannelId, ChannelArgs) ->
    shuwa_channelx:add(?TYPE, ChannelId, ?MODULE, ChannelArgs).

%% 通道初始化
init(?TYPE, ChannelId, Args) ->
    #{
        <<"ip">> := Ip,
        <<"port">> := Port,
        <<"total">> := Total} = Args,
    start_timer(20000, fun() -> start_client(Ip, Port, Total) end),
    State = #state{
        id = ChannelId
    },
    {ok, State, []}.

handle_init(State) ->
    {ok, State}.

%% 通道消息处理,注意：进程池调用
handle_event(_EventId, Event, State) ->
    lager:info("channel ~p", [Event]),
    {ok, State}.

handle_message(_Message, State) ->
    {ok, State}.

stop(ChannelType, ChannelId, _State) ->
    lager:warning("channel stop ~p,~p", [ChannelType, ChannelId]),
    ok.

start_client(Ip, Port, Total) ->
    Fun = fun(Page) ->
        timer:sleep(100),
        lists:map(fun(#{<<"devaddr">> := DevAddr, <<"product">> := #{<<"objectId">> := ProductId}, <<"name">> := Name, <<"basedata">> := #{<<"auth">> := Auth}}) ->
            dgiot_meter_device:start_connect(#{
                <<"max_reconnect">> => 10,
                <<"reconnect_times">> => 10,
                <<"ip">> => Ip,
                <<"port">> => Port,
                <<"name">> => Name,
                <<"auth">> => Auth,
                <<"productid">> => ProductId,
                <<"devaddr">> => DevAddr
            })
                  end, Page)
          end,
    Query = #{<<"where">> => #{}, <<"keys">> => [<<"devaddr">>, <<"product">>, <<"name">>, <<"basedata">>]},
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
