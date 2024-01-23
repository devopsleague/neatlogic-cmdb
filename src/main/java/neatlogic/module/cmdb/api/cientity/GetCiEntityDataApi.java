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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.cmdb.auth.label.CMDB_BASE;
import neatlogic.framework.cmdb.dto.ci.AttrVo;
import neatlogic.framework.cmdb.dto.ci.CiVo;
import neatlogic.framework.cmdb.dto.ci.RelVo;
import neatlogic.framework.cmdb.dto.cientity.AttrEntityVo;
import neatlogic.framework.cmdb.dto.cientity.CiEntityVo;
import neatlogic.framework.cmdb.dto.cientity.GlobalAttrEntityVo;
import neatlogic.framework.cmdb.dto.cientity.RelEntityVo;
import neatlogic.framework.cmdb.dto.globalattr.GlobalAttrVo;
import neatlogic.framework.cmdb.enums.RelDirectionType;
import neatlogic.framework.cmdb.enums.TransactionActionType;
import neatlogic.framework.cmdb.enums.group.GroupType;
import neatlogic.framework.cmdb.exception.cientity.CiEntityAuthException;
import neatlogic.framework.cmdb.exception.cientity.CiEntityNotFoundException;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.module.cmdb.dao.mapper.ci.AttrMapper;
import neatlogic.module.cmdb.dao.mapper.ci.CiMapper;
import neatlogic.module.cmdb.dao.mapper.ci.RelMapper;
import neatlogic.module.cmdb.dao.mapper.globalattr.GlobalAttrMapper;
import neatlogic.module.cmdb.service.ci.CiAuthChecker;
import neatlogic.module.cmdb.service.cientity.CiEntityService;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;
import java.util.Optional;

@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
@Transactional
public class GetCiEntityDataApi extends PrivateApiComponentBase {

    @Resource
    private CiEntityService ciEntityService;

    @Resource
    private CiMapper ciMapper;

    @Resource
    private RelMapper relMapper;

    @Resource
    private AttrMapper attrMapper;

    @Resource
    private GlobalAttrMapper globalAttrMapper;

    @Override
    public String getToken() {
        return "/cmdb/cientity/getdata";
    }

    @Override
    public String getName() {
        return "nmcac.getcientitydataapi.getname";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "ciEntityId", type = ApiParamType.LONG, isRequired = true, desc = "term.cmdb.cientityid")
    })
    @Output({@Param(explode = CiEntityVo.class)})
    @Description(desc = "nmcac.getcientitydataapi.getname")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        Long ciEntityId = jsonObj.getLong("ciEntityId");
        CiEntityVo ciEntityBaseVo = ciEntityService.getCiEntityBaseInfoById(ciEntityId);
        if (ciEntityBaseVo == null) {
            throw new CiEntityNotFoundException(ciEntityId);
        }
        Long ciId = ciEntityBaseVo.getCiId();
        Boolean limitRelEntity = false;
        Boolean limitAttrEntity = false;
        CiVo ciVo = ciMapper.getCiById(ciId);
        CiEntityVo pCiEntityVo = new CiEntityVo();
        pCiEntityVo.setId(ciEntityId);
        pCiEntityVo.setCiId(ciId);
        pCiEntityVo.setLimitRelEntity(limitRelEntity);
        pCiEntityVo.setLimitAttrEntity(limitAttrEntity);
        CiEntityVo entity = ciEntityService.getCiEntityById(pCiEntityVo);
        entity.setIsVirtual(ciVo.getIsVirtual());
        if (!CiAuthChecker.chain().checkCiEntityQueryPrivilege(entity.getCiId()).checkCiEntityIsInGroup(entity.getId(), GroupType.READONLY, GroupType.MAINTAIN, GroupType.AUTOEXEC).check()) {
            throw new CiEntityAuthException(entity.getCiLabel(), TransactionActionType.VIEW.getText());
        }
        List<AttrVo> attrList = attrMapper.getAttrByCiId(ciId);
        List<RelVo> relList = relMapper.getRelByCiId(ciId);
        List<GlobalAttrVo> globalAttrList = globalAttrMapper.getGlobalAttrByCiId(ciId);
        JSONObject entityObj = new JSONObject();
        entityObj.put("id", entity.getId());
        entityObj.put("uuid", entity.getUuid());
        entityObj.put("name", entity.getName());
        entityObj.put("ciId", entity.getCiId());
        entityObj.put("ciName", entity.getCiName());
        entityObj.put("ciIcon", entity.getCiIcon());
        entityObj.put("ciLabel", entity.getCiLabel());
        entityObj.put("type", entity.getTypeId());
        entityObj.put("typeName", entity.getTypeName());

        JSONArray globalAttrObjList = new JSONArray();
        if (CollectionUtils.isNotEmpty(entity.getGlobalAttrEntityList())) {
            for (GlobalAttrEntityVo attrEntityVo : entity.getGlobalAttrEntityList()) {
                JSONObject attrObj = new JSONObject();
                attrObj.put("name", attrEntityVo.getAttrName());
                attrObj.put("label", attrEntityVo.getAttrLabel());
                attrObj.put("value", attrEntityVo.getValueList());
                globalAttrObjList.add((attrObj));
            }
        }
        if (CollectionUtils.isNotEmpty(globalAttrList)) {
            for (GlobalAttrVo attr : globalAttrList) {
                if (entity.getGlobalAttrEntityList().stream().noneMatch(d -> d.getAttrId().equals(attr.getId()))) {
                    JSONObject attrObj = new JSONObject();
                    attrObj.put("name", attr.getName());
                    attrObj.put("label", attr.getLabel());
                    attrObj.put("value", new JSONArray());
                    globalAttrObjList.add((attrObj));
                }
            }
        }
        entityObj.put("globalAttrList", globalAttrObjList);

        JSONArray attrObjList = new JSONArray();
        if (CollectionUtils.isNotEmpty(entity.getAttrEntityList())) {
            for (AttrEntityVo attrEntityVo : entity.getAttrEntityList()) {
                JSONObject attrObj = new JSONObject();
                attrObj.put("id", attrEntityVo.getId());
                attrObj.put("name", attrEntityVo.getAttrName());
                attrObj.put("label", attrEntityVo.getAttrLabel());
                attrObj.put("type", attrEntityVo.getAttrType());
                if (attrEntityVo.getToCiId() != null) {
                    attrObj.put("targetCiId", attrEntityVo.getToCiId());
                }
                attrObj.put("value", attrEntityVo.getValueList());
                attrObjList.add((attrObj));
            }
        }
        //补充值为空的属性值
        if (CollectionUtils.isNotEmpty(attrList)) {
            for (AttrVo attr : attrList) {
                if (entity.getAttrEntityList().stream().noneMatch(d -> d.getAttrId().equals(attr.getId()))) {
                    JSONObject attrObj = new JSONObject();
                    attrObj.put("id", attr.getId());
                    attrObj.put("name", attr.getName());
                    attrObj.put("label", attr.getLabel());
                    attrObj.put("type", attr.getType());
                    attrObj.put("targetCiId", attr.getTargetCiId());
                    attrObj.put("value", new JSONArray());
                    attrObjList.add((attrObj));
                }
            }
        }
        entityObj.put("attrList", attrObjList);
        JSONArray relObjList = new JSONArray();
        if (CollectionUtils.isNotEmpty(entity.getRelEntityList())) {
            for (RelEntityVo relEntityVo : entity.getRelEntityList()) {
                Optional<Object> op = relObjList.stream().filter(d -> ((JSONObject) d).getLong("id").equals(relEntityVo.getRelId())).findFirst();
                JSONObject relObj;
                if (op.isPresent()) {
                    relObj = (JSONObject) op.get();
                } else {
                    relObj = new JSONObject();
                    relObj.put("value", new JSONArray());
                    relObjList.add((relObj));
                }
                relObj.put("id", relEntityVo.getRelId());
                relObj.put("name", relEntityVo.getRelName());
                relObj.put("label", relEntityVo.getRelLabel());
                relObj.put("direction", relEntityVo.getDirection());
                if (relEntityVo.getDirection().equals(RelDirectionType.FROM.getValue())) {
                    relObj.getJSONArray("value").add(relEntityVo.getToCiEntityId());
                } else {
                    relObj.getJSONArray("value").add(relEntityVo.getFromCiEntityId());
                }

            }
        }
        //补充值为空的属性值
        if (CollectionUtils.isNotEmpty(relList)) {
            for (RelVo rel : relList) {
                if (entity.getRelEntityList().stream().noneMatch(d -> d.getRelId().equals(rel.getId()))) {
                    JSONObject relObj = new JSONObject();
                    relObj.put("id", rel.getId());
                    if (rel.getDirection().equals(RelDirectionType.FROM.getValue())) {
                        relObj.put("name", rel.getToName());
                        relObj.put("label", rel.getToLabel());
                    } else {
                        relObj.put("name", rel.getFromName());
                        relObj.put("label", rel.getFromLabel());
                    }
                    relObj.put("direction", rel.getDirection());
                    relObj.put("value", new JSONArray());
                    relObjList.add((relObj));
                }
            }
        }
        entityObj.put("relList", relObjList);

        return entityObj;
    }
}