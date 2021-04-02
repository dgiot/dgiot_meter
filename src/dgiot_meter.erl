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
    get_Product/0,
    get_objectid/2,
    get_objectid/3,
    get_name/1,
    send_to_device/2,
    create_device/5,
    create_device/4,
    get_Dict/0,
    key_to_value/1,
    value_to_key/1]).

-define(APP, ?MODULE).
get_name(DtuAddr) ->
    Name = lists:foldl(fun(X, Acc) ->
        case binary:match(DtuAddr, X) of
            nomatch ->
                Acc;
            _ ->
                case binary:split(DtuAddr, [X]) of
                    [<<>>, DevAddr] ->
                        case byte_size(X) of
                            4 ->
                                {X, DevAddr, en};
                            _ ->
                                {X, DevAddr, zh}
                        end;
                    _ ->
                        Acc
                end
        end
                       end, <<>>, shuwa_data:get({suke, names})),
    case Name of
        <<>> -> {<<"名欧恩机"/utf8>>, DtuAddr, zh};
        _ -> Name
    end.

get_objectid(DtuAddr, ChannelId) ->
    {Name, DevAddr, _} = get_name(DtuAddr),
    get_objectid(Name, DevAddr, ChannelId).

get_objectid(Name, DevAddr, ChannelId) ->
    case shuwa_data:get({ChannelId, Name}) of
        not_find -> not_find;
        ProductId ->
            #{<<"objectId">> := DeviceId} =
                shuwa_parse:get_objectid(<<"Device">>, #{<<"product">> => ProductId, <<"devaddr">> => DevAddr}),
            {ProductId, DeviceId, DevAddr}
    end.

%%      SEND_SUCCESS		发送成功，但设备无执行结果返回
%%*		SEND_FAIL_OFFLINE	发送失败，设备不在线
%%*		SEND_FAIL_DISALLOW	发送失败，授权失败或设备不存在
send_to_device(Arg, _Context) ->
    #{<<"instruct_type">> := Type,
        <<"hardware_auth">> := _Auth,
        <<"hardware_number">> := DtuAddr,
        <<"hardware_path">> := _Path,
        <<"instruct_id">> := _Id,
        <<"instruct_paras">> := _Paras
    } = Arg,
    {DtuAddr2, Topic} =
        case dgiot_meter:get_name(DtuAddr) of
            {Name1, DevAddr1, zh} ->
                {ProductId, _, _} = shuwa_data:get({suke, Name1}),
                Topic1 = <<"thing/", ProductId/binary, "/", DevAddr1/binary>>,
                {DtuAddr, Topic1};
            {Name2, DevAddr2, en} ->
                {ProductId, _, _} = shuwa_data:get({suke, Name2}),
                Topic2 = <<"thing/", ProductId/binary, "/", DevAddr2/binary>>,
                #{<<"objectId">> := DeviceId} =
                    shuwa_parse:get_objectid(<<"Device">>, #{<<"product">> => ProductId, <<"devaddr">> => DevAddr2}),
                Role = case shuwa_parse:get_object(<<"Device">>, DeviceId) of
                           {ok, #{<<"ACL">> := Acl}} ->
                               NewRole = lists:foldl(fun(X, Acc) ->
                                   case X of
                                       <<"*">> -> Acc;
                                       _ ->
                                           <<"role:", Role/binary>> = X,
                                           Role
                                   end
                                                     end, <<>>, maps:keys(Acl)),
                               NewRole;
                           _ ->
                               <<"0001">>
                       end,
                DtuAddr1 = <<Name2/binary, Role/binary, DevAddr2/binary>>,
                {DtuAddr1, Topic2}
        end,
    case shuwa_utils:has_routes(Topic) of
        true ->
            Payload = term_to_binary(Arg#{<<"pid">> => self(), <<"instruct_type">> => key_to_value(Type), <<"hardware_number">> := DtuAddr2}),
            shuwa_mqtt:publish(self(), Topic, Payload),
            receive
                {hardware_msg, Msg} ->
                    #{<<"instruct_type">> := Type1, <<"instruct_paras">> := Param} = Msg,
                    {ok, Msg#{<<"instruct_type">> => value_to_key(Type1), <<"instruct_paras">> => json_to_binary(Param)}};
                {error} ->
                    {error, <<"SEND_FAIL_DISALLOW">>}
            after 10000 ->
                {error, <<"SEND_SUCCESS">>}
            end;
        false ->
            {error, <<"SEND_FAIL_OFFLINE">>}
    end.

json_to_binary(Param) ->
    Map =
        case binary:match(Param, [<<":">>]) of
            nomatch ->
                Param;
            {_, _} ->
                List = binary:split(Param, <<"+">>, [global]),
                lists:foldl(fun(X, Acc) ->
                    [_K, V] = binary:split(X, <<":">>, [global]),
                    case Acc of
                        <<>> -> <<V/binary>>;
                        _ -> <<Acc/binary, "+", V/binary>>
                    end
                            end,
                    <<>>, List)

        end,
    Map.

key_to_value(Key) ->
    case shuwa_data:get({tovalue, Key}) of
        not_find ->
            error;
        Value ->
            Value
    end.

value_to_key(Value) ->
    case shuwa_data:get({tokey, Value}) of
        not_find ->
            error;
        Key ->
            Key
    end.


get_Dict() ->
    Ids = case shuwa_parse:query_object(<<"Dict">>, #{<<"where">> => #{<<"type">> => <<"dict_template">>}}) of
              {ok, #{<<"results">> := Results}} ->
                  lists:foldl(fun(#{<<"objectId">> := ObjectId}, Acc) ->
                      case Acc of
                          <<>> -> ObjectId;
                          _ -> <<Acc/binary, ",", ObjectId/binary>>
                      end
                              end, <<>>, Results);
              _ -> <<>>
          end,
    List = binary:split(Ids, <<",">>, [global]),
    case shuwa_parse:query_object(<<"Dict">>, #{<<"where">> => #{<<"type">> => #{<<"$in">> => List}}}) of
        {ok, #{<<"results">> := Results1}} ->
            lists:foldl(fun(D, _Acc) ->
                case D of
                    #{<<"data">> := #{<<"key">> := Key, <<"value">> := Value, <<"para">> := Params}} ->
                        shuwa_data:insert({tovalue, Key}, Value),
                        shuwa_data:insert({tokey, Value}, Key),
                        shuwa_data:insert({params, Key}, Params);
                    #{<<"data">> := #{<<"key">> := Key, <<"value">> := Value}} ->
                        shuwa_data:insert({tovalue, Key}, Value),
                        shuwa_data:insert({tokey, Value}, Key);
                    _ -> pass
                end
                        end, [], Results1);
        _ -> <<>>
    end,
    ok.

get_Product() ->
    Names = case shuwa_parse:query_object(<<"Product">>, #{<<"skip">> => 0}) of
                {ok, #{<<"results">> := Results}} ->
                    lists:foldl(fun(#{<<"name">> := Name}, Acc) ->
                        Acc ++ [Name]
                                end, [], Results);
                _ -> []
            end,
    shuwa_data:insert({suke, names}, Names),
    ok.

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
                <<"basedata">> => #{<<"yysId">> => <<"09">>, <<"auth">> => <<"12345678">>},
                <<"status">> => <<"ONLINE">>,
                <<"location">> => #{<<"__type">> => <<"GeoPoint">>, <<"longitude">> => 120.161324, <<"latitude">> => 30.262441},
                <<"brand">> => <<Name/binary>>,
                <<"devModel">> => DevType
            },
            case shuwa_shadow:create_device(Requests) of
                {ok, #{<<"basedata">> := #{<<"yysId">> := YysId1, <<"auth">> := Auth}}} ->
                    shuwa_data:insert({DeviceId, sukedev}, {YysId1, Auth});
                _ ->
                    shuwa_data:insert({DeviceId, sukedev}, {<<"09">>, <<"12345678">>})
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
            {ProductId, DevType, _} = shuwa_data:get({suke, Name}),
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
                <<"basedata">> => #{<<"yysId">> => <<"09">>, <<"auth">> => <<"12345678">>},
                <<"status">> => <<"ONLINE">>,
                <<"location">> => #{<<"__type">> => <<"GeoPoint">>, <<"longitude">> => 120.161324, <<"latitude">> => 30.262441},
                <<"brand">> => <<Name/binary>>,
                <<"devModel">> => DevType
            },
            case shuwa_shadow:create_device(Requests) of
                {ok, #{<<"basedata">> := #{<<"yysId">> := YysId1, <<"auth">> := Auth}}} ->
                    shuwa_data:insert({DeviceId, sukedev}, {YysId1, Auth});
                _ ->
                    shuwa_data:insert({DeviceId, sukedev}, {<<"09">>, <<"12345678">>})
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



