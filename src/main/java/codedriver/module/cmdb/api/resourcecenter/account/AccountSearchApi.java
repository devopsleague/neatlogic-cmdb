/*
 * Copyright(c) 2021. TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.api.resourcecenter.account;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.auth.core.AuthActionChecker;
import codedriver.framework.cmdb.dto.resourcecenter.AccountVo;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.common.dto.BaseEditorVo;
import codedriver.framework.common.util.PageUtil;
import codedriver.framework.dto.OperateVo;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.cmdb.auth.label.CMDB_BASE;
import codedriver.module.cmdb.auth.label.RESOURCECENTER_ACCOUNT_MODIFY;
import codedriver.module.cmdb.dao.mapper.resourcecenter.ResourceCenterMapper;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class AccountSearchApi extends PrivateApiComponentBase {

    @Resource
    private ResourceCenterMapper resourceCenterMapper;

    @Override
    public String getToken() {
        return "resourcecenter/account/search";
    }

    @Override
    public String getName() {
        return "查询资源中心账号";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "protocol", type = ApiParamType.STRING, desc = "协议"),
            @Param(name = "keyword", type = ApiParamType.STRING, xss = true, desc = "关键词"),
            @Param(name = "currentPage", type = ApiParamType.INTEGER, desc = "当前页"),
            @Param(name = "pageSize", type = ApiParamType.INTEGER, desc = "每页数据条目"),
            @Param(name = "needPage", type = ApiParamType.BOOLEAN, desc = "是否需要分页，默认true")
    })
    @Output({
            @Param(explode = BaseEditorVo.class),
            @Param(name = "tbodyList", explode = AccountVo[].class, desc = "账号列表"),
            @Param(name = "operateList", explode = OperateVo[].class, desc = "操作列表")
    })
    @Description(desc = "查询资源中心账号")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        JSONObject resultObj = new JSONObject();
        AccountVo searchVo = JSON.toJavaObject(paramObj, AccountVo.class);
        List<AccountVo> accountVoList = resourceCenterMapper.searchAccount(searchVo);
        resultObj.put("tbodyList", accountVoList);
        if (CollectionUtils.isNotEmpty(accountVoList)) {
            Boolean hasAuth = AuthActionChecker.check(RESOURCECENTER_ACCOUNT_MODIFY.class.getSimpleName());
            accountVoList.stream().forEach(o -> {
                if (hasAuth) {
                    OperateVo delete = new OperateVo("delete", "删除");
                    if (o.getAssetsCount() > 0) {
                        delete.setDisabled(1);
                        delete.setDisabledReason("当前账号已被引用，不可删除");
                    }
                    o.getOperateList().add(delete);
                }
            });
        }
        int rowNum = resourceCenterMapper.searchAccountCount(searchVo);
        searchVo.setRowNum(rowNum);
        resultObj.put("rowNum", rowNum);
        resultObj.put("pageCount", PageUtil.getPageCount(rowNum, searchVo.getPageSize()));
        resultObj.put("currentPage", searchVo.getCurrentPage());
        resultObj.put("pageSize", searchVo.getPageSize());
        return resultObj;
    }

}