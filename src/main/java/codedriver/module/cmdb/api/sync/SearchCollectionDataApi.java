/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.api.sync;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.cmdb.dto.sync.CollectionVo;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.common.dto.BasePageVo;
import codedriver.framework.restful.annotation.Description;
import codedriver.framework.restful.annotation.Input;
import codedriver.framework.restful.annotation.OperationType;
import codedriver.framework.restful.annotation.Param;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.cmdb.auth.label.CMDB_BASE;
import codedriver.module.cmdb.dao.mapper.sync.SyncMapper;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class SearchCollectionDataApi extends PrivateApiComponentBase {
    @Resource
    private MongoTemplate mongoTemplate;

    @Resource
    private SyncMapper syncMapper;

    @Override
    public String getName() {
        return "搜索自动采集集合数据";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({@Param(name = "collection", type = ApiParamType.STRING, isRequired = true, desc = "集合名"),
            @Param(name = "keyword", type = ApiParamType.STRING, desc = "关键字"),
            @Param(name = "currentPage", type = ApiParamType.INTEGER, desc = "当前页"),
            @Param(name = "pageSize", type = ApiParamType.INTEGER, desc = "每页数量"),
            @Param(name = "keyword", type = ApiParamType.STRING, desc = "关键字")})
    @Description(desc = "搜索自动采集集合数据接口")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        String collection = paramObj.getString("collection");
        BasePageVo pageVo = JSONObject.toJavaObject(paramObj, BasePageVo.class);
        JSONObject resultObj = new JSONObject();
        CollectionVo collectionVo = mongoTemplate.findOne(new Query(Criteria.where("collection").is(collection)), CollectionVo.class, "_dictionary");
        JSONArray theadList = new JSONArray();
        Query query = new Query();
        Query countQuery = new Query();
        JSONObject subsetData = new JSONObject();
        if (collectionVo != null) {
            if (CollectionUtils.isNotEmpty(collectionVo.getFields())) {
                List<Criteria> criteriaList = new ArrayList<>();
                Pattern pattern = null;
                if (StringUtils.isNotBlank(pageVo.getKeyword())) {
                    pattern = Pattern.compile("^.*" + pageVo.getKeyword() + ".*$", Pattern.CASE_INSENSITIVE);
                    Criteria c = new Criteria();
                }
                for (int i = 0; i < collectionVo.getFields().size(); i++) {
                    JSONObject fieldObj = collectionVo.getFields().getJSONObject(i);
                    JSONObject headObj = new JSONObject();
                    headObj.put("key", fieldObj.getString("name"));
                    headObj.put("title", fieldObj.getString("desc") + "(" + fieldObj.getString("name") + ")");
                    headObj.put("className", "top");
                    theadList.add(headObj);
                    //字符串字段才启用模糊匹配
                    if (pattern != null && fieldObj.getString("type").equalsIgnoreCase("string")) {
                        criteriaList.add(Criteria.where(fieldObj.getString("name")).is(pattern));
                    }
                    if (fieldObj.containsKey("subset") && fieldObj.get("subset") instanceof List) {
                        JSONArray subTheadList = new JSONArray();
                        for (int s = 0; s < fieldObj.getJSONArray("subset").size(); s++) {
                            JSONObject subHeadObj = new JSONObject();
                            JSONObject subObj = fieldObj.getJSONArray("subset").getJSONObject(s);
                            subHeadObj.put("key", subObj.getString("name"));
                            subHeadObj.put("title", subObj.getString("desc") + "(" + subObj.getString("name") + ")");
                            subTheadList.add(subHeadObj);
                        }
                        subsetData.put(fieldObj.getString("name"), subTheadList);
                    }

                }
                if (CollectionUtils.isNotEmpty(criteriaList)) {
                    Criteria criteria = new Criteria();
                    criteria.orOperator(criteriaList);
                    query.addCriteria(criteria);
                    countQuery.addCriteria(criteria);
                }

                query.limit(pageVo.getPageSize());
                if (pageVo.getCurrentPage() > 1) {
                    query.skip((long) pageVo.getPageSize() * (pageVo.getCurrentPage() - 1));
                }
                List<JSONObject> resultList = mongoTemplate.find(query, JSONObject.class, collection);
                long rowNum = mongoTemplate.count(countQuery, collection);
                pageVo.setRowNum((int) rowNum);
                resultObj.put("theadList", theadList);
                resultObj.put("tbodyList", resultList);
                resultObj.put("subTheadData", subsetData);
                resultObj.put("currentPage", pageVo.getCurrentPage());
                resultObj.put("pageSize", pageVo.getPageSize());
                resultObj.put("pageCount", pageVo.getPageCount());
                resultObj.put("rowNum", pageVo.getRowNum());

            }
        }
        return resultObj;
    }

    @Override
    public String getToken() {
        return "/cmdb/sync/collection/data/search";
    }
}
