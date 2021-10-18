/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.api.cientity;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.cmdb.dto.cientity.RelEntityVo;
import codedriver.framework.cmdb.enums.TransactionActionType;
import codedriver.framework.cmdb.enums.group.GroupType;
import codedriver.framework.cmdb.exception.cientity.CiEntityAuthException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.Description;
import codedriver.framework.restful.annotation.Input;
import codedriver.framework.restful.annotation.OperationType;
import codedriver.framework.restful.annotation.Param;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.cmdb.auth.label.CMDB_BASE;
import codedriver.module.cmdb.dao.mapper.cientity.RelEntityMapper;
import codedriver.module.cmdb.service.ci.CiAuthChecker;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.UPDATE)
public class UpdateRelEntityValidDayApi extends PrivateApiComponentBase {

    @Autowired
    private RelEntityMapper relEntityMapper;

    @Override
    public String getToken() {
        return "/cmdb/relentity/updatevalidday";
    }

    @Override
    public String getName() {
        return "更新关系有效天数";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({@Param(name = "id", type = ApiParamType.LONG, isRequired = true, desc = "关系id"),
            @Param(name = "validDay", type = ApiParamType.INTEGER, isRequired = true, desc = "有效天数，0代表永远有效"),
    })
    @Description(desc = "更新关系有效天数接口")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        Long id = jsonObj.getLong("id");
        Integer validDay = jsonObj.getInteger("validDay");
        RelEntityVo relEntityVo = relEntityMapper.getRelEntityById(id);
        relEntityVo.setValidDay(Math.max(0, validDay));
        //关系两端随便一个有权限即可进行编辑
        if (!CiAuthChecker.chain().checkCiEntityUpdatePrivilege(relEntityVo.getFromCiId()).checkCiEntityIsInGroup(relEntityVo.getFromCiEntityId(), GroupType.MAINTAIN).check()
                && !CiAuthChecker.chain().checkCiEntityUpdatePrivilege(relEntityVo.getToCiId()).checkCiEntityIsInGroup(relEntityVo.getToCiEntityId(), GroupType.MAINTAIN).check()) {
            throw new CiEntityAuthException(TransactionActionType.UPDATE.getText());
        }
        relEntityMapper.updateRelEntityValidDay(relEntityVo);
        return null;
    }

}