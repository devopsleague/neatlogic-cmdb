package codedriver.module.cmdb.dao.mapper.cientity;

import codedriver.framework.elasticsearch.annotation.ESParam;
import codedriver.framework.elasticsearch.annotation.ESSearch;
import codedriver.module.cmdb.dto.cientity.RelEntityVo;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 *
 */
public interface RelEntityMapper {
    List<RelEntityVo> getRelEntityByRelId(Long relId);

    int checkRelEntityIsExists(RelEntityVo relEntityVo);

    List<RelEntityVo> getRelEntityByFromCiIdAndRelId(@Param("relId") Long relId, @Param("fromCiId") Long fromCiId);

    List<RelEntityVo> getRelEntityByToCiIdAndRelId(@Param("relId") Long relId, @Param("toCiId") Long toCiId);

    List<RelEntityVo> getRelEntityByCiEntityId(Long ciEntityId);

    List<RelEntityVo> searchRelEntityByCiEntityIdList(@Param("idList") List<Long> idList,
                                                      @Param("relIdList") List<Long> relIdList);

    List<RelEntityVo> getRelEntityByFromCiEntityIdAndRelId(@Param("fromCiEntityId") Long fromCiEntityId,
                                                           @Param("relId") Long relId);

    List<RelEntityVo> getRelEntityByToCiEntityIdAndRelId(@Param("toCiEntityId") Long toCiEntityId,
                                                         @Param("relId") Long relId);

    List<Long> getFromToCiEntityIdByCiEntityIdList(@Param("idList") List<Long> idList);

    List<Long> getFromToCiEntityIdByCiEntityId(Long ciEntityId);

    @ESSearch
    int insertRelEntity(@ESParam("cientity") RelEntityVo relEntityVo);

    @ESSearch
    int deleteRelEntityByFromCiEntityIdAndRelId(
            @Param("fromCiEntityId") @ESParam("cientity") Long fromCiEntityId, @Param("relId") Long relId);

    @ESSearch
    int deleteRelEntityByToCiEntityIdAndRelId(@Param("toCiEntityId") @ESParam("cientity") Long toCiEntityId,
                                              @Param("relId") Long relId);

    @ESSearch
    int deleteRelEntityByRelIdFromCiEntityIdToCiEntityId(@Param("relId") Long relId,
                                                         @Param("fromCiEntityId") @ESParam("cientity") Long fromCiEntityId,
                                                         @Param("toCiEntityId") @ESParam("cientity") Long toCiEntityId);
}
