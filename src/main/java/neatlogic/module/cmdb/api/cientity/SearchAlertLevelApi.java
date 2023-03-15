/*
 * Copyright(c) 2023 NeatLogic Co., Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neatlogic.module.cmdb.api.cientity;

import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.cmdb.auth.label.CMDB_BASE;
import neatlogic.framework.cmdb.dto.cientity.AlertLevelVo;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.module.cmdb.dao.mapper.cientity.AlertLevelMapper;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class SearchAlertLevelApi extends PrivateApiComponentBase {

    @Resource
    private AlertLevelMapper alertLevelMapper;

    @Override
    public String getToken() {
        return "/cmdb/alertlevel/search";
    }

    @Override
    public String getName() {
        return "搜索告警级别";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({})
    @Output({@Param(explode = AlertLevelVo[].class)})
    @Description(desc = "搜索告警级别接口")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        return alertLevelMapper.searchAlertLevel();
    }

}