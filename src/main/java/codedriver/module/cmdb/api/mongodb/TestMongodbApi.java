/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.api.mongodb;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.cmdb.dto.ci.AttrVo;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.cmdb.auth.label.CMDB_BASE;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class TestMongodbApi extends PrivateApiComponentBase {
    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public String getToken() {
        return "/cmdb/mongodb/test";
    }

    @Override
    public String getName() {
        return "mongo测试";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({@Param(name = "json", type = ApiParamType.JSONARRAY)
    })
    @Output({@Param(explode = AttrVo.class)})
    @Description(desc = "mongo测试接口")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        JSONArray jsonList = jsonObj.getJSONArray("json");
        for (int i = 0; i < jsonList.size(); i++) {
            mongoTemplate.insert(jsonList.getJSONObject(i), "process");
        }
        return null;
    }
}