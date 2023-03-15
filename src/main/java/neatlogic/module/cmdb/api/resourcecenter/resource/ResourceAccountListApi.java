package neatlogic.module.cmdb.api.resourcecenter.resource;

import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.cmdb.dto.resourcecenter.AccountVo;
import neatlogic.framework.cmdb.enums.resourcecenter.AccountType;
import neatlogic.framework.cmdb.exception.resourcecenter.ResourceNotFoundException;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.framework.cmdb.auth.label.CMDB_BASE;
import neatlogic.module.cmdb.dao.mapper.resourcecenter.ResourceAccountMapper;
import neatlogic.module.cmdb.dao.mapper.resourcecenter.ResourceMapper;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @author longrf
 * @date 2022/1/5 10:51 上午
 */
@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class ResourceAccountListApi extends PrivateApiComponentBase {

    @Resource
    private ResourceMapper resourceMapper;
    @Resource
    private ResourceAccountMapper resourceAccountMapper;

    @Override
    public String getName() {
        return "根据资源id查询关联的帐号信息";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Override
    public String getToken() {
        return "resourcecenter/resource/account/list";
    }

    @Input({
            @Param(name = "resourceId", type = ApiParamType.LONG, isRequired = true, desc = "资源id"),
            @Param(name = "type", type = ApiParamType.ENUM, member = AccountType.class, isRequired = true, desc = "帐号类型"),
            @Param(name = "protocol", type = ApiParamType.STRING, desc = "协议名称"),
    })
    @Output({
            @Param(explode = AccountVo[].class, desc = "帐号列表")})
    @Description(desc = "根据资产id获取关联帐号信息")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        Long resourceId = paramObj.getLong("resourceId");
        if (resourceMapper.checkResourceIsExists(resourceId) == 0) {
            throw new ResourceNotFoundException(resourceId);
        }
        String protocol = paramObj.getString("protocol");
        if (StringUtils.isNotBlank(protocol)) {
            return resourceAccountMapper.getResourceAccountListByResourceIdAndTypeAndProtocol(resourceId, paramObj.getString("type"), protocol);
        } else {
            return resourceAccountMapper.getResourceAccountListByResourceIdAndType(resourceId, paramObj.getString("type"));
        }
    }

}