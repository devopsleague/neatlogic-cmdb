/*
 * Copyright(c) 2022 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.api.sync;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.cmdb.dto.sync.CollectionVo;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.cmdb.auth.label.SYNC_MODIFY;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@AuthAction(action = SYNC_MODIFY.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class ListCollectionApi extends PrivateApiComponentBase {
    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public String getName() {
        return "获取集合列表";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({@Param(name = "needCount", type = ApiParamType.BOOLEAN, desc = "是否需要返回集合数据")})
    @Output({@Param(explode = CollectionVo[].class)})
    @Description(desc = "获取集合列表接口，需要依赖mongodb")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        List<CollectionVo> collectionList = mongoTemplate.find(new Query(), CollectionVo.class, "_dictionary");
        collectionList = collectionList.stream().distinct().collect(Collectors.toList());
        if (paramObj.getBooleanValue("needCount")) {
            if (CollectionUtils.isNotEmpty(collectionList)) {
                for (CollectionVo collectionVo : collectionList) {
                    long dataCount = mongoTemplate.count(new Query(collectionVo.getFilterCriteria()), collectionVo.getCollection());
                    collectionVo.setDataCount(dataCount);
                }
            }
        }
        return collectionList;
    }

    @Override
    public String getToken() {
        return "/cmdb/sync/collection/list";
    }
}
