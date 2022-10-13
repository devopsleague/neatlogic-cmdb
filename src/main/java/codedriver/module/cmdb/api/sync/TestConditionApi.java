/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.api.sync;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.cmdb.dto.sync.CollectionVo;
import codedriver.framework.cmdb.dto.sync.SyncCiCollectionVo;
import codedriver.framework.cmdb.dto.sync.SyncConditionVo;
import codedriver.framework.cmdb.dto.sync.SyncPolicyVo;
import codedriver.framework.cmdb.exception.sync.CollectionNotFoundException;
import codedriver.framework.cmdb.exception.sync.SyncCiCollectionNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.Description;
import codedriver.framework.restful.annotation.Input;
import codedriver.framework.restful.annotation.OperationType;
import codedriver.framework.restful.annotation.Param;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.framework.cmdb.auth.label.SYNC_MODIFY;
import codedriver.module.cmdb.dao.mapper.sync.SyncMapper;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@Service
@AuthAction(action = SYNC_MODIFY.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class TestConditionApi extends PrivateApiComponentBase {
    @Resource
    private SyncMapper syncMapper;

    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public String getName() {
        return "测试采集策略搜索条件";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({@Param(name = "collectionId", type = ApiParamType.LONG, isRequired = true, desc = "集合映射id"),
            @Param(name = "conditionList", type = ApiParamType.JSONARRAY, desc = "模型id")})
    @Description(desc = "测试采集策略搜索条件接口")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        Long collectionId = paramObj.getLong("collectionId");
        JSONArray conditionObjList = paramObj.getJSONArray("conditionList");
        List<SyncConditionVo> conditionList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(conditionObjList)) {
            for (int i = 0; i < conditionObjList.size(); i++) {
                SyncConditionVo syncConditionVo = JSONObject.toJavaObject(conditionObjList.getJSONObject(i), SyncConditionVo.class);
                conditionList.add(syncConditionVo);
            }
        }
        SyncCiCollectionVo syncCiCollectionVo = syncMapper.getSyncCiCollectionById(collectionId);
        if (syncCiCollectionVo == null) {
            throw new SyncCiCollectionNotFoundException(collectionId);
        }
        CollectionVo collectionVo = mongoTemplate.findOne(new Query().addCriteria(Criteria.where("name").is(syncCiCollectionVo.getCollectionName())), CollectionVo.class, "_dictionary");
        if (collectionVo == null) {
            throw new CollectionNotFoundException(syncCiCollectionVo.getCollectionName());
        }
        SyncPolicyVo syncPolicyVo = new SyncPolicyVo();
        syncPolicyVo.setConditionList(conditionList);
        int pageSize = 100;
        Query query = new Query();
        Criteria finalCriteria = new Criteria();
        finalCriteria.andOperator(collectionVo.getFilterCriteria(), syncPolicyVo.getCriteria());
        query.addCriteria(finalCriteria);
        query.limit(pageSize);
        return mongoTemplate.find(query, JSONObject.class, collectionVo.getCollection()).size();
    }

    @Override
    public String getToken() {
        return "/cmdb/sync/policy/condition/test";
    }
}
