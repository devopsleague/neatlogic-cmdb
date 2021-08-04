/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.api.customview;

import codedriver.framework.asynchronization.threadlocal.UserContext;
import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.auth.core.AuthActionChecker;
import codedriver.framework.cmdb.dto.customview.*;
import codedriver.framework.cmdb.enums.customview.RelType;
import codedriver.framework.cmdb.exception.customview.CustomViewCiNotFoundException;
import codedriver.framework.cmdb.exception.customview.CustomViewEmptyException;
import codedriver.framework.cmdb.exception.customview.CustomViewPrivilegeException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.cmdb.auth.label.CMDB_BASE;
import codedriver.module.cmdb.auth.label.CUSTOMVIEW_MODIFY;
import codedriver.module.cmdb.service.customview.CustomViewService;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@AuthAction(action = CMDB_BASE.class)
@OperationType(type = OperationTypeEnum.UPDATE)
@Transactional
public class SaveCustomViewApi extends PrivateApiComponentBase {

    @Autowired
    private CustomViewService customViewService;


    @Override
    public String getToken() {
        return "/cmdb/customview/save";
    }

    @Override
    public String getName() {
        return "保存自定义视图";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({@Param(name = "id", type = ApiParamType.LONG, desc = "视图id，不提供代表新增"),
            @Param(name = "name", type = ApiParamType.STRING, isRequired = true, xss = true, maxLength = 50, desc = "名称"),
            @Param(name = "icon", type = ApiParamType.STRING, desc = "图标"),
            @Param(name = "isActive", type = ApiParamType.INTEGER, isRequired = true, desc = "是否激活"),
            @Param(name = "isPrivate", type = ApiParamType.INTEGER, isRequired = true, desc = "是否私有视图"),
            @Param(name = "config", type = ApiParamType.JSONOBJECT, isRequired = true, desc = "拓扑图配置")})
    @Output({@Param(name = "Return", type = ApiParamType.LONG, desc = "视图id")})
    @Description(desc = "保存自定义视图接口")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        int isPrivate = jsonObj.getIntValue("isPrivate");
        if (isPrivate == 0) {
            if (!AuthActionChecker.check(CUSTOMVIEW_MODIFY.class.getSimpleName())) {
                throw new CustomViewPrivilegeException(CustomViewPrivilegeException.Action.SAVE);
            }
        }
        Long id = jsonObj.getLong("id");
        if (id != null) {
            CustomViewVo checkView = customViewService.getCustomViewById(id);
            if (checkView == null) {
                throw new CustomViewCiNotFoundException();
            }
            if (isPrivate == 1) {
                if (!checkView.getFcu().equalsIgnoreCase(UserContext.get().getUserUuid(true))) {
                    throw new CustomViewPrivilegeException(CustomViewPrivilegeException.Action.SAVE);
                }
            }
        }
        JSONObject config = jsonObj.getJSONObject("config");
        CustomViewVo customViewVo = JSONObject.toJavaObject(jsonObj, CustomViewVo.class);
        if (isPrivate == 1) {
            //私有视图默认都是激活
            customViewVo.setIsActive(1);
        }
        JSONArray nodes = config.getJSONArray("nodes");
        if (CollectionUtils.isEmpty(nodes)) {
            throw new CustomViewEmptyException();
        }
        Map<String, JSONObject> ciNodeMap = new HashMap<>();
        Map<String, JSONObject> attrNodeMap = new HashMap<>();
        Map<String, JSONObject> relNodeMap = new HashMap<>();
        Map<String, CustomViewAttrVo> attrMap = new HashMap<>();
        Map<String, CustomViewRelVo> relMap = new HashMap<>();
        Map<String, CustomViewCiVo> ciMap = new HashMap<>();

        List<CustomViewCiVo> ciList = new ArrayList<>();
        List<CustomViewLinkVo> linkList = new ArrayList<>();
        customViewVo.setCiList(ciList);
        customViewVo.setLinkList(linkList);
        for (int i = 0; i < nodes.size(); i++) {
            JSONObject nodeObj = nodes.getJSONObject(i);
            switch (nodeObj.getString("type")) {
                case "Ci":
                    ciNodeMap.put(nodeObj.getString("uuid"), nodeObj);
                    break;
                case "Attr":
                    attrNodeMap.put(nodeObj.getString("uuid"), nodeObj);
                    break;
                case "Rel":
                    relNodeMap.put(nodeObj.getString("uuid"), nodeObj);
                    break;
            }
        }
        JSONArray groups = config.getJSONArray("groups");
        JSONArray links = config.getJSONArray("links");

        if (CollectionUtils.isNotEmpty(groups)) {
            for (int i = 0; i < groups.size(); i++) {
                JSONObject groupObj = groups.getJSONObject(i);
                JSONArray contains = groupObj.getJSONArray("contain");
                CustomViewCiVo customViewCiVo = null;
                List<CustomViewAttrVo> attrList = new ArrayList<>();
                List<CustomViewRelVo> relList = new ArrayList<>();
                for (int j = 0; j < contains.size(); j++) {
                    if (ciNodeMap.containsKey(contains.getString(j))) {
                        JSONObject ciNodeObj = ciNodeMap.get(contains.getString(j));
                        customViewCiVo = new CustomViewCiVo(ciNodeObj);
                        customViewCiVo.setCustomViewId(customViewVo.getId());
                        ciMap.put(customViewCiVo.getUuid(), customViewCiVo);
                    } else if (attrNodeMap.containsKey(contains.getString(j))) {
                        JSONObject attrNodeObj = attrNodeMap.get(contains.getString(j));
                        CustomViewAttrVo attrVo = new CustomViewAttrVo(attrNodeObj);
                        attrVo.setCustomViewId(customViewVo.getId());
                        attrMap.put(attrVo.getUuid(), attrVo);
                        attrList.add(attrVo);
                    } else if (relNodeMap.containsKey(contains.getString(j))) {
                        JSONObject relNodeObj = relNodeMap.get(contains.getString(j));
                        CustomViewRelVo relVo = new CustomViewRelVo(relNodeObj);
                        relVo.setCustomViewId(customViewVo.getId());
                        relMap.put(relVo.getUuid(), relVo);
                        relList.add(relVo);
                    }
                }
                if (customViewCiVo != null) {
                    for (CustomViewAttrVo attrVo : attrList) {
                        attrVo.setCustomViewCiUuid(customViewCiVo.getUuid());
                    }
                    for (CustomViewRelVo relVo : relList) {
                        relVo.setCustomViewCiUuid(customViewCiVo.getUuid());
                    }
                    customViewCiVo.setAttrList(attrList);
                    customViewCiVo.setRelList(relList);
                    ciList.add(customViewCiVo);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(links)) {
            for (int i = 0; i < links.size(); i++) {
                JSONObject linkObj = links.getJSONObject(i);
                CustomViewLinkVo customViewLinkVo = new CustomViewLinkVo(linkObj);
                String sUuid = linkObj.getString("source");
                if (attrNodeMap.containsKey(sUuid)) {
                    customViewLinkVo.setFromType(RelType.ATTR.getValue());
                    customViewLinkVo.setFromCustomViewCiUuid(attrMap.get(sUuid).getCustomViewCiUuid());
                } else if (relNodeMap.containsKey(sUuid)) {
                    customViewLinkVo.setFromType(RelType.REL.getValue());
                    customViewLinkVo.setFromCustomViewCiUuid(relMap.get(sUuid).getCustomViewCiUuid());
                }

                String tUuid = linkObj.getString("target");
                if (attrNodeMap.containsKey(tUuid)) {
                    customViewLinkVo.setToType(RelType.ATTR.getValue());
                    customViewLinkVo.setToCustomViewCiUuid(attrMap.get(tUuid).getCustomViewCiUuid());
                } else if (ciNodeMap.containsKey(tUuid)) {
                    customViewLinkVo.setToType(RelType.CI.getValue());
                    customViewLinkVo.setToCustomViewCiUuid(ciMap.get(tUuid).getUuid());
                }

                linkList.add(customViewLinkVo);
            }
        }
        if (customViewVo.valid()) {
            if (id == null) {
                customViewVo.setFcu(UserContext.get().getUserUuid(true));
                customViewService.insertCustomView(customViewVo);
            } else {
                customViewVo.setLcu(UserContext.get().getUserUuid(true));
                customViewService.updateCustomView(customViewVo);
            }
            return customViewVo.getId();
        } else {
            return null;
        }
    }

}
