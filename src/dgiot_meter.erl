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
    create_device/5,
    create_device/4
]).

-define(APP, ?MODULE).

%%老设备
create_device(DeviceId, DTUIP, Name, DevAddr, Role) ->
    case shuwa_data:get({DeviceId, sukedev}) of
        not_find ->
            {ProductId, DevType, _} = shuwa_data:get({suke, Name}),
            Requests = #{
                <<"devaddr">> => DevAddr,
                <<"name">> => <<Name/binary, Role/binary, DevAddr/binary>>,
                <<"ip">> => DTUIP,
                <<"isEnable">> => true,
                <<"product">> => ProductId,
                <<"ACL">> => #{
                    <<"role:", Role/binary>> => #{
                        <<"read">> => true,
                        <<"write">> => true
                    }
                },
                <<"basedata">> => #{<<"auth">> => <<"12345678">>},
                <<"status">> => <<"ONLINE">>,
                <<"location">> => #{<<"__type">> => <<"GeoPoint">>, <<"longitude">> => 120.161324, <<"latitude">> => 30.262441},
                <<"brand">> => <<Name/binary>>,
                <<"devModel">> => DevType
            },
            case shuwa_shadow:create_device(Requests) of
                {ok, #{<<"basedata">> := #{ <<"auth">> := Auth}}} ->
                    shuwa_data:insert({DeviceId, meterdev}, {Auth});
                _ ->
                    shuwa_data:insert({DeviceId, meterdev}, {<<"12345678">>})
            end;
        _ ->
            shuwa_parse:save_to_cache(#{
                pid => self(),
                action => online,
                <<"method">> => <<"PUT">>,
                <<"path">> => <<"/classes/Device/", DeviceId/binary>>,
                <<"body">> => #{<<"ip">> => DTUIP, <<"status">> => <<"ONLINE">>}
            })
%%            shuwa_parse:update_object(<<"Device">>, DeviceId, #{<<"ip">> => DTUIP, <<"status">> => <<"ONLINE">>})
    end.

%%新设备
create_device(DeviceId, DTUIP, Name, DevAddr) ->
    {_, _, Role} = shuwa_data:get({suke, Name}),
    case shuwa_data:get({DeviceId, sukedev}) of
        not_find ->
            {ProductId, DevType, _} = shuwa_data:get({meter, Name}),
            Requests = #{
                <<"devaddr">> => DevAddr,
                <<"name">> => <<Name/binary, DevAddr/binary>>,
                <<"ip">> => DTUIP,
                <<"isEnable">> => true,
                <<"product">> => ProductId,
                <<"ACL">> => #{
                    <<"role:", Role/binary>> => #{
                        <<"read">> => true,
                        <<"write">> => true
                    }
                },
                <<"basedata">> => #{<<"auth">> => <<"12345678">>},
                <<"status">> => <<"ONLINE">>,
                <<"location">> => #{<<"__type">> => <<"GeoPoint">>, <<"longitude">> => 120.161324, <<"latitude">> => 30.262441},
                <<"brand">> => <<Name/binary>>,
                <<"devModel">> => DevType
            },
            case shuwa_shadow:create_device(Requests) of
                {ok, #{<<"basedata">> := #{<<"auth">> := Auth}}} ->
                    shuwa_data:insert({DeviceId, meterdev}, {Auth});
                _ ->
                    shuwa_data:insert({DeviceId, meterdev}, { <<"12345678">>})
            end;
        _ ->
            shuwa_parse:save_to_cache(#{
                pid => self(),
                action => online,
                <<"method">> => <<"PUT">>,
                <<"path">> => <<"/classes/Device/", DeviceId/binary>>,
                <<"body">> => #{<<"ip">> => DTUIP, <<"status">> => <<"ONLINE">>}
            })
%%            shuwa_parse:update_object(<<"Device">>, DeviceId, #{<<"ip">> => DTUIP, <<"status">> => <<"ONLINE">>})
    end.



