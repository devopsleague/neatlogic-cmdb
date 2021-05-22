/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.dao.mapper.ci;

import codedriver.framework.cmdb.dto.ci.CiTypeVo;
import codedriver.framework.cmdb.dto.ci.CiVo;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface CiMapper {

    List<CiVo> getUpwardCiListByLR(@Param("lft") Integer lft, @Param("rht") Integer rht);

    List<CiVo> getDownwardCiListByLR(@Param("lft") Integer lft, @Param("rht") Integer rht);

    List<Long> getCiNameExpressionCiIdByAttrId(Long attrId);

    List<CiVo> getAllCi();

    List<CiVo> getCiByToCiId(Long ciId);

    List<CiVo> getCiByIdList(@Param("ciIdList") List<Long> ciIds);

    int checkCiNameIsExists(CiVo ciVo);

    int checkCiLabelIsExists(CiVo ciVo);

    List<CiTypeVo> searchCiTypeCi(CiVo ciVo);

    CiVo getCiById(Long ciId);

    CiVo getCiByName(String ciName);

    int updateCi(CiVo ciVo);

    int updateCiNameExpression(@Param("ciId") Long ciId, @Param("nameExpression") String nameExpression);

    int insertCi(CiVo ciVo);

    int insertCiUnique(@Param("ciId") Long ciId, @Param("attrId") Long attrId);

    int insertCiNameExpression(@Param("ciId") Long ciId, @Param("attrId") Long attrId);

    int deleteCiById(Long ciId);

    int deleteCiUniqueByCiId(Long ciId);

    int deleteCiNameExpressionByCiId(Long ciId);
}
