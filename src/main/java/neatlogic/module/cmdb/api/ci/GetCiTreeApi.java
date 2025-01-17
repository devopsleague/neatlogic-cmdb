/*Copyright (C) 2024  深圳极向量科技有限公司 All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.*/

package neatlogic.module.cmdb.api.ci;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.auth.core.AuthActionChecker;
import neatlogic.framework.cmdb.auth.label.CMDB_BASE;
import neatlogic.framework.cmdb.dto.ci.CiVo;
import neatlogic.framework.cmdb.enums.CiAuthType;
import neatlogic.framework.cmdb.enums.group.GroupType;
import neatlogic.framework.restful.annotation.Description;
import neatlogic.framework.restful.annotation.OperationType;
import neatlogic.framework.restful.annotation.Output;
import neatlogic.framework.restful.annotation.Param;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.module.cmdb.dao.mapper.ci.CiMapper;
import neatlogic.module.cmdb.service.ci.CiAuthChecker;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Iterator;
import java.util.List;

@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class GetCiTreeApi extends PrivateApiComponentBase {

    @Resource
    private CiMapper ciMapper;


    @Override
    public String getToken() {
        return "/cmdb/ci/tree";
    }

    @Override
    public String getName() {
        return "nmcac.getciapi.getname";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Output({@Param(explode = CiVo.class)})
    @Description(desc = "nmcac.getciapi.getname")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        List<CiVo> ciList = ciMapper.getCiTree();
        //如果没有管理权限则需要检查每个模型的权限
        if (!AuthActionChecker.check("CI_MODIFY", "CIENTITY_MODIFY")) {
            Iterator<CiVo> itCi = ciList.iterator();
            while (itCi.hasNext()) {
                CiVo ciVo = itCi.next();
                if (CollectionUtils.isNotEmpty(ciVo.getAuthList())) {
                    if (!CiAuthChecker.hasPrivilege(ciVo.getAuthList(), CiAuthType.CIMANAGE, CiAuthType.CIENTITYUPDATE, CiAuthType.CIENTITYDELETE, CiAuthType.TRANSACTIONMANAGE, CiAuthType.CIENTITYQUERY)) {
                        if (!CiAuthChecker.isCiInGroup(ciVo.getId(), GroupType.READONLY, GroupType.MAINTAIN)) {
                            itCi.remove();
                        }
                    }
                } else {
                    if (!CiAuthChecker.isCiInGroup(ciVo.getId(), GroupType.READONLY, GroupType.MAINTAIN)) {
                        itCi.remove();
                    }
                }
            }
        }
        JSONArray returnList = new JSONArray();
        for (CiVo ciVo : ciList) {
            JSONObject ciObj = new JSONObject();
            ciObj.put("id", ciVo.getId());
            ciObj.put("parentCiId", ciVo.getParentCiId(true));
            ciObj.put("icon", ciVo.getIcon());
            ciObj.put("name", ciVo.getName());
            ciObj.put("label", ciVo.getLabel());
            ciObj.put("sort", ciVo.getSort());
            returnList.add(ciObj);
        }
        return returnList;
    }


}
