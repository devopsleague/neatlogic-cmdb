/*
 * Copyright(c) 2022 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.api.resourcecenter.config;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.cmdb.dto.resourcecenter.config.ResourceEntityVo;
import codedriver.framework.cmdb.enums.resourcecenter.Status;
import codedriver.framework.cmdb.exception.resourcecenter.ResourceCenterResourceFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.cmdb.auth.label.RESOURCECENTER_MODIFY;
import codedriver.module.cmdb.dao.mapper.resourcecenter.ResourceEntityMapper;
import codedriver.module.cmdb.utils.ResourceEntityViewBuilder;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Objects;

/**
 * @author linbq
 * @since 2021/11/9 11:26
 **/
@Service
@AuthAction(action = RESOURCECENTER_MODIFY.class)
@OperationType(type = OperationTypeEnum.OPERATE)
@Transactional
public class SaveResourceEntityApi extends PrivateApiComponentBase {
    @Resource
    private ResourceEntityMapper resourceEntityMapper;

    @Override
    public String getToken() {
        return "resourcecenter/resourceentity/save";
    }

    @Override
    public String getName() {
        return "保存资源配置信息";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "name", type = ApiParamType.STRING, isRequired = true, desc = "视图名"),
            @Param(name = "label", type = ApiParamType.STRING, isRequired = true, desc = "名称"),
            @Param(name = "xml", type = ApiParamType.STRING, desc = "配置信息"),
            @Param(name = "description", type = ApiParamType.STRING, desc = "描述")
    })
    @Description(desc = "保存资源配置信息接口")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        ResourceEntityVo resourceEntityVo = paramObj.toJavaObject(ResourceEntityVo.class);
        ResourceEntityVo oldResourceEntityVo = resourceEntityMapper.getResourceEntityByName(resourceEntityVo.getName());
        if (oldResourceEntityVo == null) {
            throw new ResourceCenterResourceFoundException(resourceEntityVo.getName());
        }
        boolean labelEquals = Objects.equals(resourceEntityVo.getLabel(), oldResourceEntityVo.getLabel());
        boolean xmlEquals = Objects.equals(resourceEntityVo.getXml(), oldResourceEntityVo.getXml());
        boolean descriptionEquals = Objects.equals(resourceEntityVo.getDescription(), oldResourceEntityVo.getDescription());
        if (labelEquals && xmlEquals && descriptionEquals) {
            return null;
        }
        if (!xmlEquals) {
            resourceEntityVo.setType(oldResourceEntityVo.getType());
            resourceEntityVo.setError("");
            resourceEntityVo.setStatus(Status.PENDING.getValue());
            resourceEntityMapper.updateResourceEntity(resourceEntityVo);
            if (StringUtils.isNotBlank(resourceEntityVo.getXml())) {
                ResourceEntityViewBuilder builder = new ResourceEntityViewBuilder(resourceEntityVo);
                builder.buildView();
            }
        } else {
            resourceEntityMapper.updateResourceEntityLabelAndDescription(resourceEntityVo);
        }
        return null;
    }
}