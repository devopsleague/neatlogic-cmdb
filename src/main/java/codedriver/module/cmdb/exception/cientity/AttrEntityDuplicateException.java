/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.exception.cientity;

import codedriver.framework.exception.core.ApiRuntimeException;

import java.util.List;

public class AttrEntityDuplicateException extends ApiRuntimeException {
    public AttrEntityDuplicateException(String label, List<String> valueList) {
        super("属性 " + label + " 值等于 " + String.join(",", valueList) + " 的配置项已存在");
    }

}
