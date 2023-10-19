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

package neatlogic.module.cmdb.api.ci;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.cmdb.auth.label.CMDB_BASE;
import neatlogic.framework.cmdb.dto.ci.CiSearchVo;
import neatlogic.framework.cmdb.dto.ci.CiVo;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.common.dto.BasePageVo;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.framework.util.TableResultUtil;
import neatlogic.module.cmdb.dao.mapper.ci.CiMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class SearchCiListApi extends PrivateApiComponentBase {

    @Autowired
    private CiMapper ciMapper;

    @Override
    public String getToken() {
        return "/cmdb/ci/search";
    }

    @Override
    public String getName() {
        return "nmcac.searchcilistapi.getname";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "keyword", type = ApiParamType.STRING, desc = "common.keyword"),
            @Param(name = "isAbstract", type = ApiParamType.ENUM, rule = "0,1", desc = "term.cmdb.isabstractci"),
            @Param(name = "isVirtual", type = ApiParamType.ENUM, rule = "0,1", desc = "term.cmdb.isvirtualci"),
            @Param(name = "defaultValue", type = ApiParamType.JSONARRAY, desc = "term.cmdb.ciidlist"),
            @Param(name = "currentPage", type = ApiParamType.INTEGER, desc = "common.currentpage"),
            @Param(name = "pageSize", type = ApiParamType.INTEGER, desc = "common.pagesize"),
    })
    @Output({
            @Param(explode = BasePageVo.class, desc = "common.pageinfo"),
            @Param(name = "tbodyList", explode = CiVo[].class, desc = "common.tbodylist"),
    })
    @Description(desc = "nmcac.searchcilistapi.getname")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        CiSearchVo search = jsonObj.toJavaObject(CiSearchVo.class);
        JSONArray idList = search.getDefaultValue();
        if (CollectionUtils.isNotEmpty(idList)) {
            List<Long> ciIdList = new ArrayList<>();
            for (int i = 0; i < idList.size(); i++) {
                try {
                    ciIdList.add(idList.getLong(i));
                } catch (Exception ignored) {

                }
            }
            List<CiVo> ciList = ciMapper.getCiByIdList(ciIdList);
            return TableResultUtil.getResult(ciList);
        } else {
            int rowNum = ciMapper.getCiCount(search);
            if (rowNum == 0) {
                return TableResultUtil.getResult(new ArrayList(), search);
            }
            search.setRowNum(rowNum);
            List<CiVo> ciList = ciMapper.searchCiList(search);
            return TableResultUtil.getResult(ciList, search);
        }
    }
}
