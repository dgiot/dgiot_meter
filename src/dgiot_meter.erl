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

%% @doc dgiot_meter Protocol
-module(dgiot_meter).
-include("dgiot_meter.hrl").
-export([
    create_dtu/3,
    create_meter/4,
    get_sub_device/1
]).

-define(APP, ?MODULE).

%%新设备
create_dtu(DtuAddr, ChannelId, DTUIP) ->
    lager:info("~p", [shuwa_data:get({dtu, ChannelId})]),
    case shuwa_data:get({dtu, ChannelId}) of
        {ProductId, Acl, _Properties} ->
            Requests = #{
                <<"devaddr">> => DtuAddr,
                <<"name">> => <<"DTU_", DtuAddr/binary>>,
                <<"ip">> => DTUIP,
                <<"isEnable">> => true,
                <<"product">> => ProductId,
                <<"ACL">> => Acl,
                <<"status">> => <<"ONLINE">>,
                <<"brand">> => <<"DTU", DtuAddr/binary>>,
                <<"devModel">> => <<"DTU_">>
            },
            shuwa_shadow:create_device(Requests);
        _ ->
            pass
    end.

create_meter(MeterAddr, ChannelId, DTUIP, DtuAddr) ->
    lager:info("MeterAddr ~p", [MeterAddr]),
    case shuwa_data:get({meter, ChannelId}) of
        {ProductId, ACL, _Properties} ->
            Requests = #{
                <<"devaddr">> => MeterAddr,
                <<"name">> => <<"Meter_", MeterAddr/binary>>,
                <<"ip">> => DTUIP,
                <<"isEnable">> => true,
                <<"product">> => ProductId,
                <<"ACL">> => ACL,
                <<"route">> => #{DtuAddr => MeterAddr},
                <<"status">> => <<"ONLINE">>,
                <<"brand">> => <<"Meter", MeterAddr/binary>>,
                <<"devModel">> => <<"Meter">>
            },
            shuwa_shadow:create_device(Requests),
            {DtuProductId, _, _} = shuwa_data:get({dtu, ChannelId}),
            shuwa_task:save_pnque(DtuProductId, DtuAddr, ProductId, MeterAddr);
        _ ->
            pass
    end.

get_sub_device(DtuAddr) ->
    Query = #{<<"keys">> => [<<"devaddr">>, <<"product">>],
        <<"where">> => #{<<"route.", DtuAddr/binary>> => #{<<"$regex">> => <<".+">>}},
        <<"order">> => <<"devaddr">>, <<"limit">> => 256},
    case shuwa_parse:query_object(<<"Device">>, Query) of
        {ok, #{<<"results">> := []}} -> [];
        {ok, #{<<"results">> := List}} -> List;
        _ -> []
    end.