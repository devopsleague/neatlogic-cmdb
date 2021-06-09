/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.service.customview;

import codedriver.framework.cmdb.dto.customview.CustomViewVo;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface CustomViewService {

    void updateCustomViewActive(CustomViewVo customViewVo);

    CustomViewVo getCustomViewById(Long id);

    List<CustomViewVo> searchCustomView(CustomViewVo customViewVo);

    @Transactional
    void insertCustomView(CustomViewVo customViewVo);

    @Transactional
    void updateCustomView(CustomViewVo customViewVo);

    void buildCustomView(String sql);
}