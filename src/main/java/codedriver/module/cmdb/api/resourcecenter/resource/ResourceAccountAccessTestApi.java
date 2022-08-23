/*
 * Copyright(c) 2021. TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.api.resourcecenter.resource;

import codedriver.framework.asynchronization.threadlocal.TenantContext;
import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.autoexec.exception.AutoexecJobRunnerGroupRunnerNotFoundException;
import codedriver.framework.autoexec.exception.AutoexecJobRunnerNotMatchException;
import codedriver.framework.cmdb.dto.resourcecenter.AccountVo;
import codedriver.framework.cmdb.dto.resourcecenter.ResourceVo;
import codedriver.framework.cmdb.exception.resourcecenter.ResourceAccountAccessTestException;
import codedriver.framework.cmdb.exception.resourcecenter.ResourceNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.common.util.IpUtil;
import codedriver.framework.dao.mapper.runner.RunnerMapper;
import codedriver.framework.dto.runner.GroupNetworkVo;
import codedriver.framework.dto.runner.RunnerGroupVo;
import codedriver.framework.dto.runner.RunnerMapVo;
import codedriver.framework.integration.authentication.enums.AuthenticateType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.framework.util.HttpRequestUtil;
import codedriver.module.cmdb.auth.label.CMDB_BASE;
import codedriver.module.cmdb.dao.mapper.resourcecenter.ResourceAccountMapper;
import codedriver.module.cmdb.dao.mapper.resourcecenter.ResourceMapper;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class ResourceAccountAccessTestApi extends PrivateApiComponentBase {

    @Resource
    private ResourceMapper resourceMapper;

    @Resource
    private ResourceAccountMapper resourceAccountMapper;

    @Resource
    RunnerMapper runnerMapper;

    @Override
    public String getToken() {
        return "resourcecenter/account/accesstest";
    }

    @Override
    public String getName() {
        return "测试账号可用性";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "resourceId", type = ApiParamType.LONG, isRequired = true, desc = "资源id"),
    })
    @Output({
            @Param(explode = AccountVo.class),
    })
    @Description(desc = "测试账号可用性")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        Long resourceId = paramObj.getLong("resourceId");
        ResourceVo resource = resourceMapper.getResourceById(resourceId, TenantContext.get().getDataDbName());
        if (resource == null) {
            throw new ResourceNotFoundException(resourceId);
        }
        // 根据网段查找runner
        List<RunnerMapVo> runnerMapList = null;
        List<GroupNetworkVo> networkVoList = runnerMapper.getAllNetworkMask();
        for (GroupNetworkVo networkVo : networkVoList) {
            if (IpUtil.isBelongSegment(resource.getIp(), networkVo.getNetworkIp(), networkVo.getMask())) {
                RunnerGroupVo groupVo = runnerMapper.getRunnerMapGroupById(networkVo.getGroupId());
                if (CollectionUtils.isEmpty(groupVo.getRunnerMapList())) {
                    throw new AutoexecJobRunnerGroupRunnerNotFoundException(groupVo.getName() + "(" + networkVo.getGroupId() + ") ");
                }
                runnerMapList = groupVo.getRunnerMapList();
            }
        }
        if (CollectionUtils.isEmpty(runnerMapList)) {
            throw new AutoexecJobRunnerNotMatchException();
        }
        // 随机分配runner
        int runnerMapIndex = (int) (Math.random() * runnerMapList.size());
        RunnerMapVo runnerMapVo = runnerMapList.get(runnerMapIndex);
        JSONArray list = new JSONArray();
        List<AccountVo> accountList = resourceAccountMapper.getResourceAccountListByResourceId(resourceId, null);
        if (accountList.size() > 0) {
            for (AccountVo vo : accountList) {
                list.add(new JSONObject() {
                    {
                        this.put("resourceId", resourceId);
                        this.put("host", resource.getIp() != null ? resource.getIp() : "");
                        this.put("port", vo.getProtocolPort() != null ? vo.getProtocolPort() : "");
                        this.put("protocolPort", vo.getProtocolPort() != null ? vo.getProtocolPort() : "");
                        this.put("protocol", vo.getProtocol() != null ? vo.getProtocol() : "");
                        this.put("nodeName", resource.getName() != null ? resource.getName() : "");
                        this.put("nodeType", resource.getTypeName() != null ? resource.getTypeName() : "");
                        this.put("username", vo.getAccount() != null ? vo.getAccount() : "");
                        this.put("password", vo.getPasswordCipher() != null ? vo.getPasswordCipher() : "");
                    }
                });
            }
        }
        String url = runnerMapVo.getUrl() + "api/rest/account/accesstest";
        JSONObject paramJson = new JSONObject();
        paramJson.put("accountList", list);
        HttpRequestUtil request = HttpRequestUtil.post(url).setPayload(paramJson.toJSONString()).setAuthType(AuthenticateType.BUILDIN).sendRequest();
        JSONObject resultJson = request.getResultJson();
        String error = request.getError();
        if (StringUtils.isNotBlank(error)) {
            throw new ResourceAccountAccessTestException(error);
        }
        return resultJson.getJSONArray("Return");
    }

}
