/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.publicapi;

import codedriver.framework.cmdb.attrvaluehandler.core.AttrValueHandlerFactory;
import codedriver.framework.cmdb.dto.ci.AttrVo;
import codedriver.framework.cmdb.dto.ci.CiVo;
import codedriver.framework.cmdb.dto.cientity.AttrEntityVo;
import codedriver.framework.cmdb.dto.cientity.CiEntityVo;
import codedriver.framework.cmdb.exception.ci.CiNotFoundException;
import codedriver.framework.cmdb.exception.cientity.CiEntityNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.publicapi.PublicApiComponentBase;
import codedriver.module.cmdb.dao.mapper.ci.AttrMapper;
import codedriver.module.cmdb.dao.mapper.ci.CiMapper;
import codedriver.module.cmdb.dao.mapper.cientity.CiEntityMapper;
import codedriver.module.cmdb.utils.CiEntityBuilder;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@OperationType(type = OperationTypeEnum.SEARCH)
public class GetCiEntityAttrEntityApi extends PublicApiComponentBase {

    @Resource
    private CiEntityMapper ciEntityMapper;

    @Resource
    private CiMapper ciMapper;

    @Resource
    private AttrMapper attrMapper;

    @Override
    public String getToken() {
        return "/cmdb/cientity/attrentity/get";
    }

    @Override
    public String getName() {
        return "获取配置项属性信息";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "ciEntityId", type = ApiParamType.LONG, isRequired = true, desc = "配置项id"),
            @Param(name = "attrList", type = ApiParamType.JSONARRAY, desc = "属性名称列表"),
    })
    @Output({@Param(explode = CiEntityVo.class)})
    @Description(desc = "获取配置项属性信息接口，自动化巡检时使用")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        Long ciEntityId = jsonObj.getLong("ciEntityId");
        JSONArray attrList = jsonObj.getJSONArray("attrList");
        CiEntityVo ciEntityBaseVo = ciEntityMapper.getCiEntityBaseInfoById(ciEntityId);
        if (ciEntityBaseVo == null) {
            throw new CiEntityNotFoundException(ciEntityId);
        }
        CiEntityVo ciEntityVo = getCiEntityByIdLite(ciEntityBaseVo.getCiId(), ciEntityId, attrList);

        JSONObject entityObj = new JSONObject();
        entityObj.put("id", ciEntityVo.getId());
        entityObj.put("uuid", ciEntityVo.getUuid());
        entityObj.put("name", ciEntityVo.getName());
        entityObj.put("ciId", ciEntityVo.getCiId());
        entityObj.put("type", ciEntityVo.getTypeId());
        entityObj.put("typeName", ciEntityVo.getTypeName());
        entityObj.put("attrEntityData", ciEntityVo.getAttrEntityData());
        return entityObj;
    }

    private CiEntityVo getCiEntityByIdLite(Long ciId, Long ciEntityId, JSONArray attrNameList) {
        CiVo ciVo = ciMapper.getCiById(ciId);
        if (ciVo == null) {
            throw new CiNotFoundException(ciId);
        }
        CiEntityVo ciEntityVo = new CiEntityVo();
        List<CiVo> ciList;
        if (ciVo.getIsVirtual().equals(0)) {
            ciList = ciMapper.getUpwardCiListByLR(ciVo.getLft(), ciVo.getRht());
        } else {
            ciList = new ArrayList<>();
            ciList.add(ciVo);
        }
        List<AttrVo> attrList = attrMapper.getAttrByCiId(ciVo.getId());
        attrList = attrList.stream().filter(d -> {
            for (int i = 0; i < attrNameList.size(); i++) {
                String attrName = attrNameList.getString(i);
                if (attrName.equalsIgnoreCase(d.getName())) {
                    return true;
                }
            }
            return false;
        }).collect(Collectors.toList());
        ciEntityVo.setCiList(ciList);
        ciEntityVo.setId(ciEntityId);
        ciEntityVo.setCiId(ciVo.getId());
        ciEntityVo.setCiLabel(ciVo.getLabel());
        ciEntityVo.setCiName(ciVo.getName());
        ciEntityVo.setAttrList(attrList);

        List<Map<String, Object>> resultList = ciEntityMapper.getCiEntityByIdLite(ciEntityVo);
        CiEntityVo returnCiEntityVo = new CiEntityBuilder.Builder(ciEntityVo, resultList, ciVo, attrList, null).build().getCiEntity();
        if (returnCiEntityVo != null) {
            //拼接引用属性数据
            Integer attrEntityLimit = null;
            if (CollectionUtils.isNotEmpty(attrList)) {
                for (AttrVo attrVo : attrList) {
                    if (attrVo.getTargetCiId() != null) {
                        List<AttrEntityVo> attrEntityList = ciEntityMapper.getAttrEntityByAttrIdAndFromCiEntityId(returnCiEntityVo.getId(), attrVo.getId(), attrEntityLimit);
                        if (CollectionUtils.isNotEmpty(attrEntityList)) {
                            JSONArray valueList = new JSONArray();
                            for (AttrEntityVo attrEntityVo : attrEntityList) {
                                valueList.add(attrEntityVo.getToCiEntityId());
                            }
                            JSONArray actualValueList = new JSONArray();
                            if (CollectionUtils.isNotEmpty(valueList)) {
                                actualValueList = AttrValueHandlerFactory.getHandler(attrVo.getType()).getActualValueList(attrVo, valueList);
                            }
                            returnCiEntityVo.addAttrEntityData(attrVo.getId(), CiEntityBuilder.buildAttrObj(returnCiEntityVo.getId(), attrVo, valueList, actualValueList));
                        }
                    }
                }
            }
        }
        return returnCiEntityVo;
    }

}