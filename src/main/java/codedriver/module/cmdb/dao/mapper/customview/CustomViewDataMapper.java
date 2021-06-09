/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.dao.mapper.customview;

import codedriver.framework.cmdb.dto.customview.CustomViewConditionVo;

import java.util.List;
import java.util.Map;

public interface CustomViewDataMapper {
    List<Map<String, Object>> searchCustomViewData(CustomViewConditionVo customViewConditionVo);
}