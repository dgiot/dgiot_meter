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
-module(dgiot_meter_decoder).
-include("dgiot_meter.hrl").
-author("johnliu").
-protocol([?METER]).

%% API
-export([parse_frame/2, to_frame/1]).

parse_frame(Buff, Opts) ->
    put(last, erlang:system_time(second)),
    Len = byte_size(Buff),
    NewBuff =
        case re:run(Buff, <<"[0-9a-fA-F]+">>, [global]) of
            {match, [[{0, Len}]]} -> shuwa_utils:hex_to_binary(Buff);
            _ -> Buff
        end,
    NewBuff1 = re:replace(NewBuff, <<"OK\r\n">>, <<",OK,">>, [global, {return, binary}]),
    List = binary:split(NewBuff1, <<",">>, [global]),
%%    lager:info("List ~p",[List]),
    parse_frame(List, [], Opts).


parse_frame([], Acc, _Opts) ->
    {<<>>, Acc};
parse_frame([<<"ok">>], Acc, Opts) ->
    {<<"ok">>, Acc};

parse_frame([_ | Other], Acc, Opts) ->
    parse_frame(Other, Acc, Opts).

to_frame(_) ->
    {error, not_encode}.

