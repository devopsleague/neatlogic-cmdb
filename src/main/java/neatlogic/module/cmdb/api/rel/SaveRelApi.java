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

package neatlogic.module.cmdb.api.rel;

import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.cmdb.dto.ci.RelVo;
import neatlogic.framework.cmdb.dto.ci.RelativeRelVo;
import neatlogic.framework.cmdb.exception.ci.CiAuthException;
import neatlogic.framework.cmdb.exception.ci.RelIsExistsException;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.restful.annotation.Description;
import neatlogic.framework.restful.annotation.Input;
import neatlogic.framework.restful.annotation.OperationType;
import neatlogic.framework.restful.annotation.Param;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.framework.cmdb.auth.label.CI_MODIFY;
import neatlogic.module.cmdb.dao.mapper.ci.RelMapper;
import neatlogic.module.cmdb.service.ci.CiAuthChecker;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 保存关系接口，关系不能支持虚拟模型，原因：
 * 关系支持向下关联，例如在关系A->B中，其中C是B的子模型，D是C的子模型，在编辑A模型的配置项时，A->B关系是可以选择C或D的配置项的，因此在查询关系详细信息时，sql语句不能直接join cmdb_rel表，因为cmdb_rel中的from_ci_id和to_ci_id只是记录了A和B，没有记录A和C或D的关系。
 * 现在的实现方式都是直接join cmdb_cientity表，直接拿到真实的ci_id。但虚拟模型在cmdb_cientity中是没有数据的，这导致join的时候会找不到信息，误判成关系不存在。因此综合考虑关系暂不支持虚拟模型
 */
@Service
@AuthAction(action = CI_MODIFY.class)
@Transactional
@OperationType(type = OperationTypeEnum.UPDATE)
public class SaveRelApi extends PrivateApiComponentBase {

    @Autowired
    private RelMapper relMapper;

    @Override
    public String getToken() {
        return "/cmdb/rel/save";
    }

    @Override
    public String getName() {
        return "保存模型关系";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({@Param(name = "id", type = ApiParamType.LONG, desc = "id，不存在代表新增"),
            @Param(name = "typeId", type = ApiParamType.LONG, isRequired = true, desc = "关系类型"),
            @Param(name = "inputType", type = ApiParamType.ENUM, rule = "at,mt", desc = "录入方式"),
            @Param(name = "fromLabel", type = ApiParamType.STRING, isRequired = true, xss = true, desc = "来源标签",
                    maxLength = 200),
            @Param(name = "fromTypeId", type = ApiParamType.LONG, desc = "来源类型id"),
            @Param(name = "fromGroupId", type = ApiParamType.LONG, desc = "来源分组id"),
            @Param(name = "fromRule", type = ApiParamType.ENUM, rule = "O,N", isRequired = true, desc = "来源规则"),
            @Param(name = "fromIsUnique", type = ApiParamType.INTEGER, isRequired = true, desc = "上游端是否唯一"),
            @Param(name = "fromIsRequired", type = ApiParamType.INTEGER, isRequired = true, desc = "上游端是否必填"),
            @Param(name = "fromIsCascadeDelete", type = ApiParamType.INTEGER, isRequired = true, desc = "上游端是否级联删除"),
            @Param(name = "toLabel", type = ApiParamType.STRING, isRequired = true, xss = true, desc = "目标标签",
                    maxLength = 200),
            @Param(name = "toTypeId", type = ApiParamType.LONG, desc = "目标类型id"),
            @Param(name = "toGroupId", type = ApiParamType.LONG, desc = "目标分组id"),
            @Param(name = "toRule", type = ApiParamType.ENUM, rule = "O,N", isRequired = true, desc = "目标规则"),
            @Param(name = "toIsUnique", type = ApiParamType.INTEGER, isRequired = true, desc = "下游端是否唯一"),
            @Param(name = "toIsRequired", type = ApiParamType.INTEGER, isRequired = true, desc = "下游端是否必填"),
            @Param(name = "toIsCascadeDelete", type = ApiParamType.INTEGER, isRequired = true, desc = "下游端是否级联删除"),
            @Param(name = "relativeRelList", type = ApiParamType.JSONARRAY, desc = "级联关系配置")})
    @Description(desc = "保存模型关系接口")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        RelVo relVo = JSONObject.toJavaObject(jsonObj, RelVo.class);
        Long id = jsonObj.getLong("id");
        if (relMapper.checkRelByFromToName(relVo) > 0) {
            throw new RelIsExistsException(relVo.getFromName(), relVo.getToName());
        }
        if (relMapper.checkRelByFromToLabel(relVo) > 0) {
            throw new RelIsExistsException(relVo.getFromLabel(), relVo.getToLabel());
        }
        if (!CiAuthChecker.chain().checkCiManagePrivilege(relVo.getFromCiId()).check()) {
            throw new CiAuthException(relVo.getFromLabel());
        }
        if (!CiAuthChecker.chain().checkCiManagePrivilege(relVo.getToCiId()).check()) {
            throw new CiAuthException(relVo.getToLabel());
        }
        if (id == null) {
            relMapper.insertRel(relVo);
        } else {
            relMapper.updateRel(relVo);
            relMapper.deleteRelativeRelByRelId(relVo.getId());
        }
        if (CollectionUtils.isNotEmpty(relVo.getRelativeRelList())) {
            for (RelativeRelVo relativeRelVo : relVo.getRelativeRelList()) {
                relativeRelVo.setRelId(relVo.getId());
                relMapper.insertRelativeRel(relativeRelVo);
            }
        }
        return relVo.getId();
    }

}