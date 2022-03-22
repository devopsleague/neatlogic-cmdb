/*
 * Copyright(c) 2022 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.service.customview;

import codedriver.framework.cmdb.attrvaluehandler.core.AttrValueHandlerFactory;
import codedriver.framework.cmdb.attrvaluehandler.core.IAttrValueHandler;
import codedriver.framework.cmdb.dto.ci.AttrVo;
import codedriver.framework.cmdb.dto.cientity.CiEntityVo;
import codedriver.framework.cmdb.dto.customview.*;
import codedriver.framework.cmdb.exception.customview.CustomViewNotFoundException;
import codedriver.module.cmdb.dao.mapper.customview.CustomViewDataMapper;
import codedriver.module.cmdb.dao.mapper.customview.CustomViewMapper;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class CustomViewDataServiceImpl implements CustomViewDataService {
    @Resource
    private CustomViewDataMapper customViewDataMapper;

    @Resource
    private CustomViewMapper customViewMapper;

    @Resource
    private CustomViewService customViewService;

    @Override
    public CustomViewDataVo getCustomViewData(CustomViewConditionVo customViewConditionVo) {
        CustomViewVo customViewVo = customViewMapper.getCustomViewById(customViewConditionVo.getCustomViewId());
        if (customViewVo == null) {
            throw new CustomViewNotFoundException(customViewConditionVo.getCustomViewId());
        }
        List<CustomViewAttrVo> customViewAttrList = customViewMapper.getCustomViewAttrByCustomViewId(new CustomViewAttrVo(customViewConditionVo.getCustomViewId()));
        customViewConditionVo.setFieldList(customViewAttrList.stream().map(ci -> new CustomViewConditionFieldVo(ci.getUuid(), "id")).collect(Collectors.toList()));
        CustomViewDataVo customViewDataVo = new CustomViewDataVo();
        customViewDataVo.setAttrList(customViewAttrList);
        List<Map<String, Object>> dataList = customViewDataMapper.searchCustomViewData(customViewConditionVo);
        customViewDataVo.setDataList(dataList);
        customViewDataVo.setCustomViewId(customViewVo.getId());
        customViewDataVo.setCustomViewName(customViewVo.getName());
        return customViewDataVo;
    }


    @Override
    public List<Map<String, Object>> searchCustomViewData(CustomViewConditionVo customViewConditionVo) {
        List<CustomViewAttrVo> customViewAttrList = customViewMapper.getCustomViewAttrByCustomViewId(new CustomViewAttrVo(customViewConditionVo.getCustomViewId()));
        List<CustomViewConstAttrVo> customViewConstAttrList = customViewMapper.getCustomViewConstAttrByCustomViewId(new CustomViewConstAttrVo(customViewConditionVo.getCustomViewId()));
        Map<String, AttrVo> attrMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(customViewAttrList)) {
            for (CustomViewAttrVo customViewAttr : customViewAttrList) {
                attrMap.put(customViewAttr.getUuid(), customViewAttr.getAttrVo());
                if (MapUtils.isNotEmpty(customViewAttr.getCondition())) {
                    String expression = customViewAttr.getCondition().getString("expression");
                    if (StringUtils.isNotBlank(expression)) {
                        JSONArray valueList = customViewAttr.getCondition().getJSONArray("valueList");
                        customViewConditionVo.addAttrFilter(new CustomViewConditionFilterVo(customViewAttr.getUuid(), expression, valueList));
                    }
                }
            }
        }
        if (CollectionUtils.isNotEmpty(customViewConstAttrList)) {
            for (CustomViewConstAttrVo customViewConstAttrVo : customViewConstAttrList) {

            }
        }
        List<CustomViewConditionFieldVo> customViewConditionFieldList = new ArrayList<>();
        customViewConditionFieldList.addAll(customViewAttrList.stream().map(attr -> new CustomViewConditionFieldVo(attr.getUuid(), "attr")).collect(Collectors.toList()));
        customViewConditionFieldList.addAll(customViewConstAttrList.stream().map(attr -> new CustomViewConditionFieldVo(attr.getUuid(), "constattr")).collect(Collectors.toList()));
        customViewConditionVo.setFieldList(customViewConditionFieldList);

        List<Map<String, Object>> dataList = customViewDataMapper.searchCustomViewData(customViewConditionVo);
        if (CollectionUtils.isNotEmpty(customViewConditionVo.getValueFilterList())) {
            for (Map<String, Object> data : dataList) {
                //必须要复制一份，否则序列化成json会出错
                List<CustomViewValueFilterVo> filterList = new ArrayList<>();
                for (CustomViewValueFilterVo filterVo : customViewConditionVo.getValueFilterList()) {
                    filterList.add(new CustomViewValueFilterVo(filterVo.getUuid(), filterVo.getValue()));
                }
                data.put("_filterList", filterList);

            }
        }

        //转换属性真实值
        for (Map<String, Object> data : dataList) {
            for (String key : data.keySet()) {
                if (!key.equals("id") && !key.endsWith("_hash") && attrMap.containsKey(key) && data.get(key) != null) {
                    IAttrValueHandler handler = AttrValueHandlerFactory.getHandler(attrMap.get(key).getType());
                    if (handler != null) {
                        JSONArray vl = new JSONArray();
                        vl.add(data.get(key));
                        handler.transferValueListToDisplay(attrMap.get(key), vl);
                        if (CollectionUtils.isNotEmpty(vl)) {
                            data.put(key, vl.get(0));
                        }
                    }
                }
            }
        }
        return dataList;
    }

    @Override
    public CustomViewVo getCustomViewCiEntityById(CustomViewConditionVo customViewConditionVo) {
        CustomViewVo customViewVo = customViewService.getCustomViewDetailById(customViewConditionVo.getCustomViewId());
        if (customViewVo == null) {
            throw new CustomViewNotFoundException(customViewConditionVo.getCustomViewId());
        }
        List<CustomViewConditionFieldVo> fieldList = new ArrayList<>();
        fieldList.addAll(customViewVo.getCiList().stream().map(ci -> new CustomViewConditionFieldVo(ci.getUuid(), "id")).collect(Collectors.toList()));
        fieldList.addAll(customViewVo.getCiList().stream().map(ci -> new CustomViewConditionFieldVo(ci.getUuid(), "name")).collect(Collectors.toList()));
        customViewConditionVo.setFieldList(fieldList);
        if (CollectionUtils.isNotEmpty(customViewConditionVo.getFieldList())) {
            List<Map<String, Object>> resultList = customViewDataMapper.getCustomViewCiEntityById(customViewConditionVo);
            for (Map<String, Object> result : resultList) {
                for (String key : result.keySet()) {
                    if (key.contains("_id")) {
                        String ciUuid = key.replace("_id", "");
                        CustomViewCiVo ciVo = customViewVo.getCustomCiByUuid(ciUuid);
                        if (ciVo != null) {
                            ciVo.addCiEntity((Long) result.get(key), (String) result.get(ciUuid + "_name"));
                        }
                    }
                }
            }
        }
        //清空不需要返回的信息
        customViewVo.setConfig(null);
        customViewVo.setConfigStr(null);
        return customViewVo;
    }

    @Override
    public List<CiEntityVo> searchCustomViewCiEntity(CustomViewConditionVo customViewConditionVo) {
        List<CustomViewAttrVo> customViewAttrList = customViewMapper.getCustomViewAttrByCustomViewId(new CustomViewAttrVo(customViewConditionVo.getCustomViewId()));
        List<CustomViewConstAttrVo> customViewConstAttrList = customViewMapper.getCustomViewConstAttrByCustomViewId(new CustomViewConstAttrVo(customViewConditionVo.getCustomViewId()));
        List<CustomViewConditionFieldVo> customViewConditionFieldList = new ArrayList<>();
        customViewConditionFieldList.addAll(customViewAttrList.stream().map(attr -> new CustomViewConditionFieldVo(attr.getUuid(), "attr")).collect(Collectors.toList()));
        customViewConditionFieldList.addAll(customViewConstAttrList.stream().map(attr -> new CustomViewConditionFieldVo(attr.getUuid(), "constattr")).collect(Collectors.toList()));
        customViewConditionVo.setFieldList(customViewConditionFieldList);
        return customViewDataMapper.searchCustomViewCiEntity(customViewConditionVo);
    }

    @Override
    public List<CustomViewDataGroupVo> searchCustomViewDataGroup(CustomViewConditionVo customViewConditionVo) {
        CustomViewAttrVo customViewAttrVo = customViewMapper.getCustomViewAttrByUuid(customViewConditionVo.getGroupBy());
        CustomViewConstAttrVo customViewConstAttrVo = customViewMapper.getCustomViewConstAttrByUuid(customViewConditionVo.getGroupBy());
        List<CustomViewAttrVo> customViewAttrList = customViewMapper.getCustomViewAttrByCustomViewId(new CustomViewAttrVo(customViewConditionVo.getCustomViewId()));
        List<CustomViewConstAttrVo> customViewConstAttrList = customViewMapper.getCustomViewConstAttrByCustomViewId(new CustomViewConstAttrVo(customViewConditionVo.getCustomViewId()));
        Map<String, AttrVo> attrMap = new HashMap<>();
        //Map<String, CustomViewConstAttrVo> constAttrMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(customViewAttrList)) {
            for (CustomViewAttrVo customViewAttr : customViewAttrList) {
                attrMap.put(customViewAttr.getUuid(), customViewAttr.getAttrVo());
                if (MapUtils.isNotEmpty(customViewAttr.getCondition())) {
                    String expression = customViewAttr.getCondition().getString("expression");
                    if (StringUtils.isNotBlank(expression)) {
                        JSONArray valueList = customViewAttr.getCondition().getJSONArray("valueList");
                        customViewConditionVo.addAttrFilter(new CustomViewConditionFilterVo(customViewAttr.getUuid(), expression, valueList));
                    }
                }
            }
        }
        List<CustomViewConditionFieldVo> customViewConditionFieldList = new ArrayList<>();
        customViewConditionFieldList.addAll(customViewAttrList.stream().map(attr -> new CustomViewConditionFieldVo(attr.getUuid(), "attr")).collect(Collectors.toList()));
        customViewConditionFieldList.addAll(customViewConstAttrList.stream().map(attr -> new CustomViewConditionFieldVo(attr.getUuid(), "constattr")).collect(Collectors.toList()));

        customViewConditionVo.setFieldList(customViewConditionFieldList);

        List<CustomViewDataGroupVo> groupList = customViewDataMapper.searchCustomViewDataGroup(customViewConditionVo);
        for (CustomViewDataGroupVo customViewDataGroupVo : groupList) {
            if (customViewAttrVo != null) {
                customViewDataGroupVo.setAttrAlias(customViewAttrVo.getAlias());
                customViewDataGroupVo.setAttrUuid(customViewAttrVo.getUuid());
            } else if (customViewConstAttrVo != null) {
                customViewDataGroupVo.setAttrAlias(customViewConstAttrVo.getAlias());
                customViewDataGroupVo.setAttrUuid(customViewConstAttrVo.getUuid());
            }
            if (CollectionUtils.isNotEmpty(customViewConditionVo.getValueFilterList())) {
                //必须要复制一份，否则序列化成json会出错
                for (CustomViewValueFilterVo filterVo : customViewConditionVo.getValueFilterList()) {
                    customViewDataGroupVo.addValueFilter(new CustomViewValueFilterVo(filterVo.getUuid(), filterVo.getValue()));
                }
            }
            //转换属性真实值
            if (customViewAttrVo != null) {
                if (attrMap.containsKey(customViewAttrVo.getUuid())) {
                    IAttrValueHandler handler = AttrValueHandlerFactory.getHandler(attrMap.get(customViewAttrVo.getUuid()).getType());
                    if (handler != null) {
                        JSONArray vl = new JSONArray();
                        vl.add(customViewDataGroupVo.getValue());
                        handler.transferValueListToDisplay(attrMap.get(customViewAttrVo.getUuid()), vl);
                        if (CollectionUtils.isNotEmpty(vl)) {
                            customViewDataGroupVo.setValue(vl.getString(0));
                        }
                    }
                }
            }
        }
        return groupList;
    }

}
