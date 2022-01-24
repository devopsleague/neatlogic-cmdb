/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.api.cientity;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.cmdb.crossover.IBatchDeleteCiEntityApiCrossoverService;
import codedriver.framework.cmdb.dto.cientity.CiEntityVo;
import codedriver.framework.cmdb.enums.TransactionActionType;
import codedriver.framework.cmdb.enums.group.GroupType;
import codedriver.framework.cmdb.exception.cientity.CiEntityAuthException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.Description;
import codedriver.framework.restful.annotation.Input;
import codedriver.framework.restful.annotation.OperationType;
import codedriver.framework.restful.annotation.Output;
import codedriver.framework.restful.annotation.Param;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.cmdb.auth.label.CIENTITY_MODIFY;
import codedriver.module.cmdb.auth.label.CI_MODIFY;
import codedriver.module.cmdb.auth.label.CMDB_BASE;
import codedriver.module.cmdb.service.ci.CiAuthChecker;
import codedriver.module.cmdb.service.cientity.CiEntityService;

@Service
@AuthAction(action = CMDB_BASE.class)
@AuthAction(action = CI_MODIFY.class)
@AuthAction(action = CIENTITY_MODIFY.class)
@OperationType(type = OperationTypeEnum.DELETE)
public class BatchDeleteCiEntityApi extends PrivateApiComponentBase implements IBatchDeleteCiEntityApiCrossoverService {

    @Autowired
    private CiEntityService ciEntityService;

    @Override
    public String getToken() {
        return "/cmdb/cientity/batchdelete";
    }

    @Override
    public String getName() {
        return "批量删除配置项";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({@Param(name = "ciEntityList", type = ApiParamType.JSONARRAY, isRequired = true, desc = "删除列表，包含ciId、ciEntityId和ciEntityName三个属性"),
            @Param(name = "needCommit", type = ApiParamType.BOOLEAN, isRequired = true, desc = "是否需要提交"),
            @Param(name = "description", type = ApiParamType.STRING, desc = "备注", xss = true)})
    @Output({@Param(name = "transactionGroupId", type = ApiParamType.LONG, desc = "事务组id")})
    @Description(desc = "批量删除配置项接口")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        String description = jsonObj.getString("description");
        JSONArray ciEntityObjList = jsonObj.getJSONArray("ciEntityList");
        boolean needCommit = jsonObj.getBooleanValue("needCommit");
        List<CiEntityVo> ciEntityList = new ArrayList<>();
        for (int i = 0; i < ciEntityObjList.size(); i++) {
            JSONObject data = ciEntityObjList.getJSONObject(i);
            Long ciId = data.getLong("ciId");
            Long ciEntityId = data.getLong("ciEntityId");
            String ciEntityName = data.getString("ciEntityName");
            CiEntityVo ciEntityVo = new CiEntityVo();
            ciEntityVo.setId(ciEntityId);
            ciEntityVo.setDescription(description);
            ciEntityList.add(ciEntityVo);
            if (!CiAuthChecker.chain().checkCiEntityDeletePrivilege(ciId).checkCiEntityIsInGroup(ciEntityId, GroupType.MAINTAIN).check()) {
                throw new CiEntityAuthException(ciEntityId, ciEntityName, TransactionActionType.DELETE.getText());
            }
            if (needCommit) {
                needCommit = CiAuthChecker.chain().checkCiEntityTransactionPrivilege(ciId).checkCiEntityIsInGroup(ciEntityId, GroupType.MAINTAIN).check();
                if (!needCommit) {
                    throw new CiEntityAuthException(ciEntityId, ciEntityName, TransactionActionType.DELETE.getText());
                }
            }
        }
        return ciEntityService.deleteCiEntityList(ciEntityList, needCommit);
    }

}
