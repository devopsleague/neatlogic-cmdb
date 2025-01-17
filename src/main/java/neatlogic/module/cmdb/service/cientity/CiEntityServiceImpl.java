/*Copyright (C) 2024  深圳极向量科技有限公司 All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.*/

package neatlogic.module.cmdb.service.cientity;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import neatlogic.framework.asynchronization.threadlocal.InputFromContext;
import neatlogic.framework.asynchronization.threadlocal.UserContext;
import neatlogic.framework.cmdb.attrvaluehandler.core.AttrValueHandlerFactory;
import neatlogic.framework.cmdb.attrvaluehandler.core.IAttrValueHandler;
import neatlogic.framework.cmdb.crossover.ICiEntityCrossoverService;
import neatlogic.framework.cmdb.dto.attrexpression.RebuildAuditVo;
import neatlogic.framework.cmdb.dto.ci.AttrVo;
import neatlogic.framework.cmdb.dto.ci.CiViewVo;
import neatlogic.framework.cmdb.dto.ci.CiVo;
import neatlogic.framework.cmdb.dto.ci.RelVo;
import neatlogic.framework.cmdb.dto.cientity.*;
import neatlogic.framework.cmdb.dto.globalattr.GlobalAttrFilterVo;
import neatlogic.framework.cmdb.dto.globalattr.GlobalAttrItemVo;
import neatlogic.framework.cmdb.dto.globalattr.GlobalAttrVo;
import neatlogic.framework.cmdb.dto.transaction.*;
import neatlogic.framework.cmdb.enums.*;
import neatlogic.framework.cmdb.enums.group.GroupType;
import neatlogic.framework.cmdb.exception.attr.AttrNotFoundException;
import neatlogic.framework.cmdb.exception.attrtype.AttrTypeNotFoundException;
import neatlogic.framework.cmdb.exception.ci.CiNotFoundException;
import neatlogic.framework.cmdb.exception.ci.CiUniqueAttrNotFoundException;
import neatlogic.framework.cmdb.exception.ci.CiUniqueRuleException;
import neatlogic.framework.cmdb.exception.cientity.*;
import neatlogic.framework.cmdb.exception.globalattr.GlobalAttrValueIrregularException;
import neatlogic.framework.cmdb.exception.transaction.TransactionAuthException;
import neatlogic.framework.cmdb.exception.transaction.TransactionStatusIrregularException;
import neatlogic.framework.cmdb.utils.RelUtil;
import neatlogic.framework.cmdb.validator.core.IValidator;
import neatlogic.framework.cmdb.validator.core.ValidatorFactory;
import neatlogic.framework.fulltextindex.core.FullTextIndexHandlerFactory;
import neatlogic.framework.fulltextindex.core.IFullTextIndexHandler;
import neatlogic.framework.mq.core.ITopic;
import neatlogic.framework.mq.core.TopicFactory;
import neatlogic.framework.transaction.core.AfterTransactionJob;
import neatlogic.framework.util.$;
import neatlogic.module.cmdb.attrexpression.AttrExpressionRebuildManager;
import neatlogic.module.cmdb.dao.mapper.ci.AttrMapper;
import neatlogic.module.cmdb.dao.mapper.ci.CiMapper;
import neatlogic.module.cmdb.dao.mapper.ci.CiViewMapper;
import neatlogic.module.cmdb.dao.mapper.ci.RelMapper;
import neatlogic.module.cmdb.dao.mapper.cientity.AttrEntityMapper;
import neatlogic.module.cmdb.dao.mapper.cientity.CiEntityMapper;
import neatlogic.module.cmdb.dao.mapper.cientity.RelEntityMapper;
import neatlogic.module.cmdb.dao.mapper.globalattr.GlobalAttrMapper;
import neatlogic.module.cmdb.dao.mapper.transaction.TransactionMapper;
import neatlogic.module.cmdb.fulltextindex.enums.CmdbFullTextIndexType;
import neatlogic.module.cmdb.group.CiEntityGroupManager;
import neatlogic.module.cmdb.process.exception.DataConversionAppendPathException;
import neatlogic.module.cmdb.relativerel.RelativeRelManager;
import neatlogic.module.cmdb.service.ci.CiAuthChecker;
import neatlogic.module.cmdb.utils.CiEntityBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class CiEntityServiceImpl implements CiEntityService, ICiEntityCrossoverService {
    private static final Logger logger = LoggerFactory.getLogger(CiEntityServiceImpl.class);
    private static final String EXPRESSION_TYPE = "expression";

    @Resource
    private CiEntityMapper ciEntityMapper;

    @Resource
    private GlobalAttrMapper globalAttrMapper;

    @Resource
    private RelEntityMapper relEntityMapper;

    @Resource
    private AttrEntityMapper attrEntityMapper;

    @Resource
    private TransactionMapper transactionMapper;


    @Resource
    private CiMapper ciMapper;

    @Resource
    private AttrMapper attrMapper;

    @Resource
    private RelMapper relMapper;

    @Resource
    private CiViewMapper ciViewMapper;

    @Override
    public CiEntityVo getCiEntityBaseInfoById(Long ciEntityId) {
        return ciEntityMapper.getCiEntityBaseInfoById(ciEntityId);
    }

    @Override
    public String getCiEntityNameByCiEntityId(Long ciEntityId) {
        CiEntityVo entity = ciEntityMapper.getCiEntityBaseInfoById(ciEntityId);
        if (entity != null) {
            return entity.getName();
        }
        return null;
    }

    @Override
    public List<CiEntityVo> getCiEntityBaseInfoByName(Long ciId, String name) {
        CiVo ciVo = ciMapper.getCiById(ciId);
        if (ciVo == null) {
            return new ArrayList<>();
        }
        CiEntityVo search = new CiEntityVo();
        search.setName(name);
        if (ciVo.getIsVirtual().equals(0)) {
            // 非虚拟模型
            List<CiVo> downwardCiList = ciMapper.getDownwardCiListByLR(ciVo.getLft(), ciVo.getRht());
            Map<Long, CiVo> downwardCiMap = downwardCiList.stream().collect(Collectors.toMap(CiVo::getId, e -> e));
            search.setIdList(new ArrayList<>(downwardCiMap.keySet()));
            return ciEntityMapper.getCiEntityListByCiIdListAndName(search);
        } else {
            // 虚拟模型
            search.setCiId(ciVo.getId());
            return ciEntityMapper.getVirtualCiEntityBaseInfoByName(search);
        }
    }

    /**
     * 精简版查询单个配置项，不会join关系和属性表，在应用层通过多次search进行数据拼接
     *
     * @param ciId            模型id
     * @param ciEntityId      配置项id
     * @param flattenAttr     是否返回空属性
     * @param limitRelEntity  是否限制关系数量
     * @param limitAttrEntity 是否限制引用属性数量
     * @return 配置项信息
     */
    private CiEntityVo getCiEntityByIdLite(Long ciId, Long ciEntityId, Boolean flattenAttr, Boolean limitRelEntity, Boolean limitAttrEntity) {
        CiVo ciVo = ciMapper.getCiById(ciId);
        if (ciVo == null) {
            throw new CiNotFoundException(ciId);
        }
        CiEntityVo ciEntityVo = new CiEntityVo();
        List<CiVo> ciList;
        if (ciVo.getIsVirtual().equals(0)) {
            ciList = ciMapper.getUpwardCiListByLR(ciVo.getLft(), ciVo.getRht());
        } else {
            ciList = new ArrayList<>();
            ciList.add(ciVo);
        }
        List<AttrVo> attrList = attrMapper.getAttrByCiId(ciVo.getId());
        List<RelVo> relList = RelUtil.ClearRepeatRel(relMapper.getRelByCiId(ciVo.getId()));
        ciEntityVo.setCiList(ciList);
        ciEntityVo.setId(ciEntityId);
        ciEntityVo.setCiId(ciVo.getId());
        ciEntityVo.setCiLabel(ciVo.getLabel());
        ciEntityVo.setCiName(ciVo.getName());

        ciEntityVo.setAttrList(attrList);
        ciEntityVo.setRelList(relList);
        ciEntityVo.setLimitRelEntity(limitRelEntity);
        ciEntityVo.setLimitAttrEntity(limitAttrEntity);

        List<HashMap<String, Object>> resultList = ciEntityMapper.getCiEntityByIdLite(ciEntityVo);
        CiEntityVo returnCiEntityVo = new CiEntityBuilder.Builder(ciEntityVo, resultList, ciVo, attrList, relList).isFlattenAttr(flattenAttr).build().getCiEntity();
        if (returnCiEntityVo != null) {
            //获取全局属性数据
            GlobalAttrVo ga = new GlobalAttrVo();
            ga.setIsActive(1);
            List<GlobalAttrVo> activeGlobalAttrList = globalAttrMapper.searchGlobalAttr(ga);
            List<GlobalAttrEntityVo> globalAttrList = globalAttrMapper.getGlobalAttrByCiEntityId(ciEntityVo.getId());
            for (GlobalAttrVo globalAttrVo : activeGlobalAttrList) {
                Optional<GlobalAttrEntityVo> op = globalAttrList.stream().filter(d -> d.getAttrId().equals(globalAttrVo.getId())).findFirst();
                returnCiEntityVo.addGlobalAttrData(globalAttrVo.getId(), CiEntityBuilder.buildGlobalAttrObj(returnCiEntityVo.getId(), globalAttrVo, op.map(GlobalAttrEntityVo::getValueList).orElse(null)));
            }
            //拼接引用属性数据
            Long attrEntityLimit = null;
            if (Boolean.TRUE.equals(limitAttrEntity)) {
                attrEntityLimit = CiEntityVo.MAX_ATTRENTITY_COUNT;
            }
            if (CollectionUtils.isNotEmpty(attrList)) {
                for (AttrVo attrVo : attrList) {
                    if (attrVo.getTargetCiId() != null) {
                        List<AttrEntityVo> attrEntityList = ciEntityMapper.getAttrEntityByAttrIdAndFromCiEntityId(returnCiEntityVo.getId(), attrVo.getId(), attrEntityLimit != null ? attrEntityLimit + 1 : null);
                        if (CollectionUtils.isNotEmpty(attrEntityList)) {
                            JSONArray valueList = new JSONArray();
                            for (AttrEntityVo attrEntityVo : attrEntityList) {
                                valueList.add(attrEntityVo.getToCiEntityId());
                            }
                            JSONArray actualValueList = new JSONArray();
                            if (CollectionUtils.isNotEmpty(valueList)) {
                                actualValueList = AttrValueHandlerFactory.getHandler(attrVo.getType()).getActualValueList(attrVo, valueList);
                            }
                            returnCiEntityVo.addAttrEntityData(attrVo.getId(), CiEntityBuilder.buildAttrObj(returnCiEntityVo.getId(), attrVo, valueList, actualValueList));
                        }
                    }
                }
            }
            //拼接关系数据
            Long relEntityLimit = null;
            if (Boolean.TRUE.equals(limitRelEntity)) {
                relEntityLimit = CiEntityVo.MAX_RELENTITY_COUNT;
            }
            if (CollectionUtils.isNotEmpty(relList)) {
                for (RelVo relVo : relList) {
                    List<RelEntityVo> relEntityList;
                    if (relVo.getDirection().equals(RelDirectionType.FROM.getValue())) {
                        relEntityList = relEntityMapper.getRelEntityByFromCiEntityIdAndRelId(returnCiEntityVo.getId(), relVo.getId(), relEntityLimit != null ? relEntityLimit + 1 : null);
                    } else {
                        relEntityList = relEntityMapper.getRelEntityByToCiEntityIdAndRelId(returnCiEntityVo.getId(), relVo.getId(), relEntityLimit != null ? relEntityLimit + 1 : null);
                    }
                    if (CollectionUtils.isNotEmpty(relEntityList)) {
                        returnCiEntityVo.addRelEntityData(relVo.getId(), relVo.getDirection(), CiEntityBuilder.buildRelObj(returnCiEntityVo.getId(), relVo, relEntityList));
                    }
                }
            }
        }
        return returnCiEntityVo;
    }


    @Override
    public CiEntityVo getCiEntityById(Long ciId, Long ciEntityId) {
        return getCiEntityByIdLite(ciId, ciEntityId, false, true, true);
    }

    @Override
    public CiEntityVo getCiEntityById(CiEntityVo ciEntityVo) {
        return getCiEntityByIdLite(ciEntityVo.getCiId(), ciEntityVo.getId(), false, ciEntityVo.isLimitRelEntity(), ciEntityVo.isLimitAttrEntity());
    }

    @Override
    public List<CiEntityVo> getCiEntityByIdList(Long ciId, List<Long> ciEntityIdList) {
        CiEntityVo ciEntityVo = new CiEntityVo();
        ciEntityVo.setCiId(ciId);
        ciEntityVo.setIdList(ciEntityIdList);
        ciEntityVo.setLimitAttrEntity(true);
        ciEntityVo.setLimitRelEntity(true);
        return getCiEntityByIdList(ciEntityVo);
    }


    @Override
    public List<CiEntityVo> getCiEntityByIdList(CiEntityVo ciEntityVo) {
        if (CollectionUtils.isNotEmpty(ciEntityVo.getIdList())) {
            List<CiVo> belongCiList = new ArrayList<>();
            List<CiEntityVo> ciEntityList = new ArrayList<>();
            if (ciEntityVo.getCiId() != null) {
                CiVo ciVo = ciMapper.getCiById(ciEntityVo.getCiId());
                if (ciVo == null) {
                    throw new CiNotFoundException(ciEntityVo.getCiId());
                }
                belongCiList.add(ciVo);
            } else {
                belongCiList = ciMapper.getCiBaseInfoByCiEntityIdList(ciEntityVo.getIdList());
            }
            for (CiVo ciVo : belongCiList) {
                List<CiVo> ciList = ciMapper.getUpwardCiListByLR(ciVo.getLft(), ciVo.getRht());
                List<AttrVo> attrList = attrMapper.getAttrByCiId(ciVo.getId());
                List<RelVo> relList = RelUtil.ClearRepeatRel(relMapper.getRelByCiId(ciVo.getId()));
                ciEntityVo.setCiList(ciList);
                ciEntityVo.setAttrList(attrList);
                ciEntityVo.setRelList(relList);
                if (CollectionUtils.isNotEmpty(ciEntityVo.getIdList())) {
                    List<HashMap<String, Object>> resultList = ciEntityMapper.searchCiEntity(ciEntityVo);
                    ciEntityList.addAll(new CiEntityBuilder.Builder(ciEntityVo, resultList, ciVo, attrList, relList).build().getCiEntityList());
                }
            }
            return ciEntityList;
        }
        return new ArrayList<>();
    }

    @Override
    public List<Long> getCiEntityIdByCiId(CiEntityVo ciEntityVo) {
        return ciEntityMapper.getCiEntityIdByCiId(ciEntityVo);
    }

    @Override
    public List<CiEntityVo> searchCiEntity(CiEntityVo ciEntityVo) {
        long time = 0L;
        if (logger.isInfoEnabled()) {
            time = System.currentTimeMillis();
        }

        CiVo ciVo = ciMapper.getCiById(ciEntityVo.getCiId());
        if (ciVo == null) {
            throw new CiNotFoundException(ciEntityVo.getCiId());
        }
        List<CiVo> ciList = ciMapper.getUpwardCiListByLR(ciVo.getLft(), ciVo.getRht());
        List<AttrVo> attrList = attrMapper.getAttrByCiId(ciVo.getId());
        List<RelVo> relList = RelUtil.ClearRepeatRel(relMapper.getRelByCiId(ciVo.getId()));

        if (CollectionUtils.isNotEmpty(ciEntityVo.getExcludeRelIdList())) {
            relList.removeIf(d -> ciEntityVo.getExcludeRelIdList().contains(d.getId()));
        }
        //把条件的最大限制设到关系里
        for (RelVo relVo : relList) {
            relVo.setMaxRelEntityCount(ciEntityVo.getMaxRelEntityCount());
        }
        for (AttrVo attrVo : attrList) {
            attrVo.setMaxAttrEntityCount(ciEntityVo.getMaxAttrEntityCount());
        }

        ciEntityVo.setCiList(ciList);
        ciEntityVo.setAttrList(attrList);
        ciEntityVo.setRelList(relList);
        /*
        如果有属性过滤，则根据属性补充关键信息
         */
        if (CollectionUtils.isNotEmpty(ciEntityVo.getAttrFilterList())) {
            Iterator<AttrFilterVo> itAttrFilter = ciEntityVo.getAttrFilterList().iterator();
            while (itAttrFilter.hasNext()) {
                AttrFilterVo attrFilterVo = itAttrFilter.next();
                boolean isExists = false;
                for (AttrVo attrVo : attrList) {
                    if (attrVo.getId().equals(attrFilterVo.getAttrId())) {
                        attrFilterVo.setCiId(attrVo.getCiId());
                        attrFilterVo.setType(attrVo.getType());
                        attrFilterVo.setNeedTargetCi(attrVo.isNeedTargetCi());
                        isExists = true;
                        break;
                    }
                }
                if (!isExists) {
                    itAttrFilter.remove();
                }
            }
        }
        if (CollectionUtils.isNotEmpty(ciEntityVo.getRelFilterList())) {
            Iterator<RelFilterVo> itRelFilter = ciEntityVo.getRelFilterList().iterator();
            while (itRelFilter.hasNext()) {
                RelFilterVo relFilterVo = itRelFilter.next();
                boolean isExists = false;
                for (RelVo relVo : relList) {
                    if (relVo.getId().equals(relFilterVo.getRelId()) && relVo.getDirection().equals(relFilterVo.getDirection())) {
                        isExists = true;
                        break;
                    }
                }
                if (!isExists) {
                    itRelFilter.remove();
                }
            }
        }
        Boolean isLimitRelEntity = ciEntityVo.isLimitRelEntity();
        Boolean isLimitAttrEntity = ciEntityVo.isLimitAttrEntity();
        if (ciEntityVo.getIdList() == null) {
            ciEntityVo.setLimitRelEntity(false);
            ciEntityVo.setLimitAttrEntity(false);
            if (ciEntityVo.getNeedRowNum()) {
                int rowNum = ciEntityMapper.searchCiEntityIdCount(ciEntityVo);
                if (logger.isInfoEnabled()) {
                    logger.info("查询配置项行数，行数{}，耗时{}ms", rowNum, System.currentTimeMillis() - time);
                }
                ciEntityVo.setRowNum(rowNum);
            }

            List<Long> ciEntityIdList = ciEntityMapper.searchCiEntityId(ciEntityVo);
            if (logger.isInfoEnabled()) {
                logger.info("查询配置项id列表，行数{}，耗时{}ms", ciEntityIdList.size(), System.currentTimeMillis() - time);
            }
            if (CollectionUtils.isNotEmpty(ciEntityIdList)) {
                ciEntityVo.setIdList(ciEntityIdList);
            }
        }
        if (CollectionUtils.isNotEmpty(ciEntityVo.getIdList())) {
            ciEntityVo.setLimitRelEntity(isLimitRelEntity != null ? isLimitRelEntity : true);
            ciEntityVo.setLimitAttrEntity(isLimitAttrEntity != null ? isLimitAttrEntity : true);
            List<HashMap<String, Object>> resultList = ciEntityMapper.searchCiEntity(ciEntityVo);
            if (logger.isInfoEnabled()) {
                logger.info("根据id查询配置项，行数{}，耗时{}ms", resultList.size(), System.currentTimeMillis() - time);
            }
            List<GlobalAttrEntityVo> globalAttrList = globalAttrMapper.getGlobalAttrByCiEntityIdList(ciEntityVo.getIdList());
            ciEntityVo.setIdList(null);//清除id列表，避免ciEntityVo重用时数据没法更新
            List<CiEntityVo> ciEntityList = new CiEntityBuilder.Builder(ciEntityVo, resultList, ciVo, attrList, relList).build().getCiEntityList();
            if (CollectionUtils.isNotEmpty(globalAttrList)) {
                for (CiEntityVo cientity : ciEntityList) {
                    List<GlobalAttrEntityVo> tmpAttrList = globalAttrList.stream().filter(d -> d.getCiEntityId().equals(cientity.getId())).collect(Collectors.toList());
                    if (CollectionUtils.isNotEmpty(tmpAttrList)) {
                        for (GlobalAttrEntityVo attr : tmpAttrList) {
                            cientity.addGlobalAttrData(attr.getAttrId(), attr.toJSONObject());
                        }
                    }
                }
            }
            if (logger.isInfoEnabled()) {
                logger.info("查询配置项总耗时，数据量{}，耗时{}ms", ciEntityList.size(), System.currentTimeMillis() - time);
            }
            return ciEntityList;
        }
        return new ArrayList<>();
    }

    @Override
    public List<CiEntityVo> searchCiEntityBaseInfo(CiEntityVo ciEntityVo) {
        List<CiEntityVo> ciEntityList = ciEntityMapper.searchCiEntityBaseInfo(ciEntityVo);
        if (CollectionUtils.isNotEmpty(ciEntityList)) {
            int rowNum = ciEntityMapper.searchCiEntityBaseInfoCount(ciEntityVo);
            ciEntityVo.setRowNum(rowNum);
        }
        return ciEntityList;
    }

    /**
     * 删除配置项
     *
     * @param ciEntityVo 配置项
     * @return 事务id
     */
    @Transactional
    @Override
    public Long deleteCiEntity(CiEntityVo ciEntityVo, Boolean allowCommit) {
        TransactionGroupVo transactionGroupVo = new TransactionGroupVo();
        Long transactionId = deleteCiEntity(ciEntityVo, allowCommit, transactionGroupVo);
        if (transactionId > 0L) {
            transactionMapper.insertTransactionGroup(transactionGroupVo.getId(), transactionId);
        }
        return transactionId;
    }

    /**
     * 批量删除配置项
     *
     * @param ciEntityList 配置项列表
     * @param allowCommit  是否允许提交
     * @return 事务组id
     */
    @Transactional
    @Override
    public Long deleteCiEntityList(List<CiEntityVo> ciEntityList, Boolean allowCommit) {
        if (CollectionUtils.isNotEmpty(ciEntityList)) {
            TransactionGroupVo transactionGroupVo = new TransactionGroupVo();
            for (CiEntityVo ciEntityVo : ciEntityList) {
                Long transactionId = deleteCiEntity(ciEntityVo, allowCommit, transactionGroupVo);
                if (transactionId > 0L) {
                    transactionMapper.insertTransactionGroup(transactionGroupVo.getId(), transactionId);
                }
            }
            return transactionGroupVo.getId();
        }
        return 0L;
    }

    /**
     * 删除配置项
     *
     * @param ciEntityVo 配置项
     * @return 事务id，为0代表没有创建新事务
     */
    @Transactional
    @Override
    public Long deleteCiEntity(CiEntityVo ciEntityVo, Boolean allowCommit, TransactionGroupVo transactionGroupVo) {
        Long ciEntityId = ciEntityVo.getId();
        CiEntityVo baseCiEntityVo = this.ciEntityMapper.getCiEntityBaseInfoById(ciEntityId);
        if (baseCiEntityVo == null) {
            throw new CiEntityNotFoundException(ciEntityId);
        }
        //检查是否有未提交的删除事务，如果有就不再创建新事务
        List<TransactionVo> transactionList = transactionMapper.getUnCommitTransactionByCiEntityIdAndAction(ciEntityId, TransactionActionType.DELETE.getValue());
        if (CollectionUtils.isNotEmpty(transactionList)) {
            throw new CiEntityHasUnCommitTransactionException(baseCiEntityVo, TransactionActionType.DELETE);
            /*if (!allowCommit) {
                return 0L;//没有创建新事务
            } else {
                //如果需要提交
                for (TransactionVo transactionVo : transactionList) {
                    TransactionGroupVo oldTransactionGroup = transactionMapper.getTransactionGroupByTransactionId(transactionVo.getId());
                    commitTransaction(transactionVo, oldTransactionGroup);
                }
            }*/
        }

        CiEntityVo oldCiEntityVo = this.getCiEntityByIdLite(baseCiEntityVo.getCiId(), ciEntityId, true, false, false);

        //如果作为属性被引用，则不能删除
        List<AttrVo> attrList = ciEntityMapper.getAttrListByToCiEntityId(ciEntityId);
        if (CollectionUtils.isNotEmpty(attrList)) {
            throw new CiEntityIsInUsedException(baseCiEntityVo, attrList);
        }

        TransactionVo transactionVo = new TransactionVo();
        transactionVo.setCiId(oldCiEntityVo.getCiId());
        transactionVo.setInputFrom(InputFromContext.get().getInputFrom());
        transactionVo.setStatus(TransactionStatus.UNCOMMIT.getValue());
        transactionVo.setCreateUser(UserContext.get().getUserUuid(true));
        transactionVo.setDescription(ciEntityVo.getDescription());
        CiEntityTransactionVo ciEntityTransactionVo = new CiEntityTransactionVo(oldCiEntityVo);
        ciEntityTransactionVo.setAction(TransactionActionType.DELETE.getValue());
        ciEntityTransactionVo.setTransactionId(transactionVo.getId());
        ciEntityTransactionVo.setOldCiEntityVo(oldCiEntityVo);
        transactionVo.setCiEntityTransactionVo(ciEntityTransactionVo);

        // 保存快照
        createSnapshot(ciEntityTransactionVo);

        // 写入事务
        transactionMapper.insertTransaction(transactionVo);
        // 写入配置项事务
        transactionMapper.insertCiEntityTransaction(ciEntityTransactionVo);
        if (allowCommit) {
            commitTransaction(transactionVo, transactionGroupVo);
        }

        return transactionVo.getId();

    }


    @Override
    @Transactional
    public Long saveCiEntity(List<CiEntityTransactionVo> ciEntityTransactionList) {
        TransactionGroupVo transactionGroupVo = new TransactionGroupVo();
        return saveCiEntity(ciEntityTransactionList, transactionGroupVo);
    }

    @Override
    public Long saveCiEntityWithoutTransaction(List<CiEntityTransactionVo> ciEntityTransactionList, TransactionGroupVo transactionGroupVo) {
        for (CiEntityTransactionVo ciEntityTransactionVo : ciEntityTransactionList) {
            transactionGroupVo.addExclude(ciEntityTransactionVo.getCiEntityId());
        }
        if (CollectionUtils.isNotEmpty(ciEntityTransactionList)) {
            //批量更新时为了防止后续更新干扰，需要提前生成所有配置项的snapshot
            for (int i = ciEntityTransactionList.size() - 1; i >= 0; i--) {
                CiEntityTransactionVo ciEntityTransactionVo = ciEntityTransactionList.get(i);
                if (ciEntityTransactionVo.getAction().equals(TransactionActionType.UPDATE.getValue())) {
                    CiEntityVo oldCiEntityVo = this.getCiEntityByIdLite(ciEntityTransactionVo.getCiId(), ciEntityTransactionVo.getCiEntityId(), true, false, false);
                    /*
                    正在编辑中的配置项，在事务提交或删除前不允许再次修改
                     */
                    if (oldCiEntityVo == null) {
                        /*
                        旧配置项不存在且action==UPDATE，证明此配置项是新配置项，只是被多次引用导致后续添加的都是UPDATE操作，此种配置项应该从事务列表中删除，什么都不需要处理
                         */
                        ciEntityTransactionList.remove(i);
                        continue;
                    } else if (oldCiEntityVo.getIsLocked().equals(1)) {
                        throw new CiEntityIsLockedException(ciEntityTransactionVo.getCiEntityId());
                    }
                    ciEntityTransactionVo.setOldCiEntityVo(oldCiEntityVo);
                    // 生成快照
                    createSnapshot(ciEntityTransactionVo);
                }
            }
            for (CiEntityTransactionVo ciEntityTransactionVo : ciEntityTransactionList) {
                try {
                    Long transactionId = saveCiEntity(ciEntityTransactionVo, transactionGroupVo);
                    if (transactionId > 0L) {
                        transactionMapper.insertTransactionGroup(transactionGroupVo.getId(), transactionId);
                    }
                } catch (Exception e) {
                    if (CollectionUtils.isNotEmpty(ciEntityTransactionVo.getConfigurationPathList())
                            || CollectionUtils.isNotEmpty(ciEntityTransactionVo.getActualPathList())) {
                        String configurationPath = "";
                        if (CollectionUtils.isNotEmpty(ciEntityTransactionVo.getConfigurationPathList())) {
                            configurationPath = String.join(",", ciEntityTransactionVo.getConfigurationPathList());
                        }
                        String actualPath = "";
                        if (CollectionUtils.isNotEmpty(ciEntityTransactionVo.getActualPathList())) {
                            actualPath = String.join(",", ciEntityTransactionVo.getActualPathList());
                        }
                        throw new DataConversionAppendPathException(e, configurationPath, actualPath);
                    } else {
                        throw e;
                    }
                }
            }
        }
        return transactionGroupVo.getId();
    }

    @Override
    @Transactional
    public Long saveCiEntity(List<CiEntityTransactionVo> ciEntityTransactionList, TransactionGroupVo transactionGroupVo) {
        return saveCiEntityWithoutTransaction(ciEntityTransactionList, transactionGroupVo);
    }

    @Transactional
    @Override
    public Long saveCiEntity(CiEntityTransactionVo ciEntityTransactionVo) {
        return saveCiEntity(ciEntityTransactionVo, new TransactionGroupVo());
    }


    /**
     * 保存单个配置项事务
     *
     * @param ciEntityTransactionVo 事务
     * @param transactionGroupVo    事务分组
     * @return 事务id
     */
    @Transactional
    @Override
    public Long saveCiEntity(CiEntityTransactionVo ciEntityTransactionVo, TransactionGroupVo transactionGroupVo) {
        //批量更新时会生成snapshot，但有些地方还需要在这里生成snapshot
        if (ciEntityTransactionVo.getAction().equals(TransactionActionType.UPDATE.getValue()) && ciEntityTransactionVo.getOldCiEntityVo() == null) {
            CiEntityVo oldCiEntityVo = this.getCiEntityByIdLite(ciEntityTransactionVo.getCiId(), ciEntityTransactionVo.getCiEntityId(), true, false, false);
            // 正在编辑中的配置项，在事务提交或删除前不允许再次修改
            if (oldCiEntityVo == null) {
                //配置项不存在直接返回0L，代表什么都不需要做
                return 0L;
                //throw new CiEntityNotFoundException(ciEntityTransactionVo.getCiEntityId());
            } else if (oldCiEntityVo.getIsLocked().equals(1)) {
                throw new CiEntityIsLockedException(ciEntityTransactionVo.getCiEntityId());
            }
            ciEntityTransactionVo.setOldCiEntityVo(oldCiEntityVo);

            // 生成快照
            createSnapshot(ciEntityTransactionVo);
        }
        //锁定当前配置项
        if (ciEntityTransactionVo.getOldCiEntityVo() != null) {
            ciEntityTransactionVo.getOldCiEntityVo().setIsLocked(1);
            ciEntityMapper.updateCiEntityLockById(ciEntityTransactionVo.getOldCiEntityVo());
        }

        //如果是添加配置项，则锁定模型，防止高并发时导致重复数据添加进来
        if (ciEntityTransactionVo.getAction().equals(TransactionActionType.INSERT.getValue())) {
            ciMapper.getCiLock(ciEntityTransactionVo.getCiId());
        }

        TransactionVo transactionVo = new TransactionVo();
        transactionVo.setCiId(ciEntityTransactionVo.getCiId());
        transactionVo.setInputFrom(InputFromContext.get().getInputFrom());
        transactionVo.setStatus(TransactionStatus.UNCOMMIT.getValue());
        transactionVo.setCreateUser(UserContext.get().getUserUuid(true));
        transactionVo.setDescription(ciEntityTransactionVo.getDescription());
        ciEntityTransactionVo.setTransactionId(transactionVo.getId());

        transactionVo.setCiEntityTransactionVo(ciEntityTransactionVo);
        boolean hasChange = validateCiEntityTransaction(ciEntityTransactionVo);

        if (hasChange) {
            //生成配置项名称
            createCiEntityName(ciEntityTransactionVo);

            // 写入事务
            transactionMapper.insertTransaction(transactionVo);
            // 写入配置项事务
            transactionMapper.insertCiEntityTransaction(ciEntityTransactionVo);
            //提交事务
            if (ciEntityTransactionVo.isAllowCommit()) {
                commitTransaction(transactionVo, transactionGroupVo);
            }
            return transactionVo.getId();
        } else {
            // 没有任何变化则返回零
            return 0L;
        }
    }

    private void createCiEntityName(CiEntityTransactionVo ciEntityTransactionVo) {
        CiVo ciVo = ciMapper.getCiById(ciEntityTransactionVo.getCiId());
        List<AttrEntityTransactionVo> attrEntityList = ciEntityTransactionVo.getAttrEntityTransactionList();
        if (ciVo.getNameAttrId() != null) {
            //如果事务中存在名称属性
            Optional<AttrEntityTransactionVo> op = attrEntityList.stream().filter(attr -> attr.getAttrId().equals(ciVo.getNameAttrId())).findFirst();
            if (op.isPresent()) {
                AttrEntityTransactionVo attrEntityTransactionVo = op.get();
                if (Boolean.TRUE.equals(attrEntityTransactionVo.isNeedTargetCi())) {
                    List<Long> invokeCiEntityIdList = new ArrayList<>();
                    for (int i = 0; i < attrEntityTransactionVo.getValueList().size(); i++) {
                        try {
                            invokeCiEntityIdList.add(attrEntityTransactionVo.getValueList().getLong(i));
                        } catch (Exception ignored) {

                        }
                    }
                    Set<CiEntityVo> invokeCiEntitySet = new HashSet<>();
                    if (CollectionUtils.isNotEmpty(invokeCiEntityIdList)) {
                        invokeCiEntitySet.addAll(ciEntityMapper.getCiEntityBaseInfoByIdList(invokeCiEntityIdList));
                    }
                    if (ciEntityTransactionVo.getAction().equals(TransactionActionType.UPDATE.getValue()) && attrEntityTransactionVo.getSaveMode().equals(SaveModeType.MERGE.getValue())) {
                        invokeCiEntitySet.addAll(ciEntityMapper.getCiEntityBaseInfoByAttrIdAndFromCiEntityId(ciEntityTransactionVo.getCiEntityId(), attrEntityTransactionVo.getAttrId()));
                    }
                    if (CollectionUtils.isNotEmpty(invokeCiEntitySet)) {
                        ciEntityTransactionVo.setName(invokeCiEntitySet.stream().map(CiEntityVo::getName).collect(Collectors.joining(",")));
                    } else {
                        ciEntityTransactionVo.setName("");
                    }
                } else {
                    ciEntityTransactionVo.setName(attrEntityTransactionVo.getValue());
                }
            } else {
                //找不到直接使用旧名字
                CiEntityVo oldCiEntityVo = ciEntityMapper.getCiEntityBaseInfoById(ciEntityTransactionVo.getCiEntityId());
                if (oldCiEntityVo != null) {
                    ciEntityTransactionVo.setName(oldCiEntityVo.getName());
                }
            }
        }
    }

    @Override
    public void updateCiEntityName(CiEntityVo ciEntityVo) {
        ciEntityMapper.updateCiEntityName(ciEntityVo);
    }

    @Override
    public void updateCiEntityNameForCi(CiVo ciVo) {
        if (ciVo.getNameAttrId() != null && Objects.equals(0, ciVo.getIsAbstract())) {
            CiEntityVo pCiEntityVo = new CiEntityVo();
            pCiEntityVo.setCiId(ciVo.getId());
            pCiEntityVo.setPageSize(100);
            pCiEntityVo.setCurrentPage(1);
            List<Long> attrIdList = new ArrayList<>();
            attrIdList.add(ciVo.getNameAttrId());
            pCiEntityVo.setAttrIdList(attrIdList);

            List<Long> relIdList = new ArrayList<>();
            relIdList.add(0L);
            pCiEntityVo.setRelIdList(relIdList);

            List<CiEntityVo> ciEntityList = searchCiEntity(pCiEntityVo);
            while (CollectionUtils.isNotEmpty(ciEntityList)) {
                for (CiEntityVo ciEntityVo : ciEntityList) {
                    String ciEntityName = "";
                    for (AttrEntityVo attrEntityVo : ciEntityVo.getAttrEntityList()) {
                        if (attrEntityVo.getAttrId().equals(ciVo.getNameAttrId())) {
                            ciEntityName = attrEntityVo.getActualValueList().stream().map(Object::toString).collect(Collectors.joining(","));
                            break;
                        }
                    }
                    ciEntityVo.setName(ciEntityName);
                    ciEntityMapper.updateCiEntityName(ciEntityVo);
                }
                pCiEntityVo.setCurrentPage(pCiEntityVo.getCurrentPage() + 1);
                ciEntityList = searchCiEntity(pCiEntityVo);
            }
        }
    }

    @Override
    public void createSnapshot(CiEntityTransactionVo ciEntityTransactionVo) {
        CiEntityVo oldCiEntityVo = ciEntityTransactionVo.getOldCiEntityVo();
        if (oldCiEntityVo != null) {
            String content = JSON.toJSONString(oldCiEntityVo);
            ciEntityTransactionVo.setSnapshot(content);
        }
    }


    /**
     * 验证配置项数据是否合法
     * 属性必填校验：
     * 1、编辑模式是Global模式，不提供的属性代表要删除，需要校验是否满足必填。
     * 2、编辑模式是Partial模式，不提供的属性代表不修改，所以不需要做校验。
     * 3、如果是Merge模式，并且是引用型属性，需要先获取旧属性，如果新值旧值同时为空才需要抛异常。
     * 4、如果是replace模式，新值为空则直接抛异常。
     * <p>
     * 检查属性是否唯一：
     * 1、如果是引用值，则存在多值的可能性，需要根据保存模式来组装数据进行校验。
     * 如果是replace模式，直接用新值进行校验。
     * 如果是Merge模式，则需要先把新值和旧值合并后再进行校验。
     * 2、如果是普通属性值，只有单值的可能，不管什么模式只需要使用新值进行校验即可。
     * <p>
     * 检查唯一规则：
     * 1、如果唯一规则包含引用值，
     * <p>
     * 记录属性是新增、编辑或删除：
     * 1、如果整个attr_xxxx在snapshot中不存在，代表添加了新属性，如果attr_xxxx的valueList在snapshot中为空，代表属性添加了值。
     * 2、如果snapshot本身已存在attr_xxx和valueList，代表编辑。
     * 3、如果修改模式是Partial，attr_xxx为空，代表删除整个属性，如果只是valueList清空，代表属性删除了值，其他不提供的代表不动。
     *
     * @param ciEntityTransactionVo 配置项事务实体
     * @return true:验证成功 false:验证失败
     */
    @Override
    public boolean validateCiEntityTransaction(CiEntityTransactionVo ciEntityTransactionVo) {
        List<AttrVo> attrList = attrMapper.getAttrByCiId(ciEntityTransactionVo.getCiId());
        List<RelVo> relList = RelUtil.ClearRepeatRel(relMapper.getRelByCiId(ciEntityTransactionVo.getCiId()));
        List<GlobalAttrVo> globalAttrList = globalAttrMapper.searchGlobalAttr(new GlobalAttrVo() {{
            this.setIsActive(1);
        }});
        CiVo ciVo = ciMapper.getCiById(ciEntityTransactionVo.getCiId());
        //如果外部有自定义唯一规则，则使用外部的唯一规则
        if (CollectionUtils.isNotEmpty(ciEntityTransactionVo.getUniqueAttrIdList())) {
            ciVo.setUniqueAttrIdList(ciEntityTransactionVo.getUniqueAttrIdList());
        }
        CiEntityVo oldEntity = ciEntityTransactionVo.getOldCiEntityVo();
        List<AttrEntityVo> oldAttrEntityList = null;
        List<RelEntityVo> oldRelEntityList = null;
        List<GlobalAttrEntityVo> oldGlobalAttrEntityList = null;
        if (oldEntity == null) {
            //如果是单纯校验可能会没有旧配置项信息，这里重新获取一次
            oldEntity = this.getCiEntityByIdLite(ciEntityTransactionVo.getCiId(), ciEntityTransactionVo.getCiEntityId(), true, false, false);
        }
        if (oldEntity != null) {
            oldAttrEntityList = oldEntity.getAttrEntityList() == null ? new ArrayList<>() : oldEntity.getAttrEntityList();
            oldRelEntityList = oldEntity.getRelEntityList() == null ? new ArrayList<>() : oldEntity.getRelEntityList();
            oldGlobalAttrEntityList = oldEntity.getGlobalAttrEntityList() == null ? new ArrayList<>() : oldEntity.getGlobalAttrEntityList();
        }

        // 清除和模型属性不匹配的属性
        if (MapUtils.isNotEmpty(ciEntityTransactionVo.getAttrEntityData())) {
            for (AttrVo attrVo : attrList) {
                JSONObject attrEntityData = ciEntityTransactionVo.getAttrEntityDataByAttrId(attrVo.getId());
                if (attrEntityData != null) {
                    //修正属性基本信息，多余属性不要
                    JSONArray valueList = attrEntityData.getJSONArray("valueList");
                    //进行必要的值转换，例如密码转换成密文
                    IAttrValueHandler handler = AttrValueHandlerFactory.getHandler(attrVo.getType());
                    handler.transferValueListToSave(attrVo, valueList);
                    String saveMode = attrEntityData.getString("saveMode");
                    attrEntityData.clear();
                    attrEntityData.put("saveMode", saveMode);
                    attrEntityData.put("valueList", valueList);
                    attrEntityData.put("label", attrVo.getLabel());
                    attrEntityData.put("name", attrVo.getName());
                    attrEntityData.put("type", attrVo.getType());
                    attrEntityData.put("ciId", attrVo.getCiId());
                    attrEntityData.put("targetCiId", attrVo.getTargetCiId());
                } /*else {
                    //全局模式下，不提供属性代表需要删除，因此直接补充valueList为空的属性数据
                    // FIXME 按照设计在global模式下，不提供的属性代表删除，但资源清单某些修改配置项的页面属性是不全的，会导致不提供的属性全部清空，要先确认没问题才补充以下逻辑
                    if (ciEntityTransactionVo.getEditMode().equals(EditModeType.GLOBAL.getValue())) {
                        ciEntityTransactionVo.addAttrEntityData(attrVo);
                    }
                }*/
            }
        }

        // 清除和当前全局属性不匹配的全局属性
        if (MapUtils.isNotEmpty(ciEntityTransactionVo.getGlobalAttrEntityData())) {
            for (GlobalAttrVo globalAttrVo : globalAttrList) {
                JSONObject globalAttrEntityData = ciEntityTransactionVo.getGlobalAttrEntityDataByAttrId(globalAttrVo.getId());
                if (globalAttrEntityData != null) {
                    //修正属性基本信息，多余属性不要

                    JSONArray valueList = globalAttrEntityData.getJSONArray("valueList");
                    if (globalAttrVo.getIsMultiple().equals(0) && valueList.size() > 1) {
                        throw new GlobalAttrValueIrregularException.MultipleException(globalAttrVo);
                    }
                    globalAttrEntityData.clear();
                    globalAttrEntityData.put("valueList", valueList);
                    globalAttrEntityData.put("label", globalAttrVo.getLabel());
                    globalAttrEntityData.put("name", globalAttrVo.getName());
                    globalAttrEntityData.put("id", globalAttrVo.getId());
                } /*else {
                    //全局模式下，不提供属性代表需要删除，因此直接补充valueList为空的属性数据
                    //FIXME 按照设计在global模式下，不提供的属性代表删除，但资源清单某些修改配置项的页面属性是不全的，会导致不提供的属性全部清空，要先确认没问题才补充以下逻辑
                    //if (ciEntityTransactionVo.getEditMode().equals(EditModeType.GLOBAL.getValue())) {
                    //    ciEntityTransactionVo.addGlobalAttrEntityData(globalAttrVo);
                    //}
                }*/
            }
        }

        // 消除和模型属性不匹配的关系
        if (MapUtils.isNotEmpty(ciEntityTransactionVo.getRelEntityData())) {
            for (RelVo relVo : relList) {
                JSONObject relEntityData = ciEntityTransactionVo.getRelEntityDataByRelIdAndDirection(relVo.getId(), relVo.getDirection());
                if (relEntityData != null) {
                    if (!relEntityData.containsKey("valueList") || CollectionUtils.isEmpty(relEntityData.getJSONArray("valueList"))) {
                        ciEntityTransactionVo.removeRelEntityData(relVo.getId(), relVo.getDirection());
                    } else {
                        //补充关系基本信息
                        String direction = relVo.getDirection();
                        relEntityData.put("name", direction.equals(RelDirectionType.FROM.getValue()) ? relVo.getToName() : relVo.getFromName());
                        relEntityData.put("label", direction.equals(RelDirectionType.FROM.getValue()) ? relVo.getToLabel() : relVo.getFromLabel());
                        relEntityData.put("direction", direction);
                        relEntityData.put("fromCiId", relVo.getFromCiId());
                        relEntityData.put("toCiId", relVo.getToCiId());
                        for (int i = relEntityData.getJSONArray("valueList").size() - 1; i >= 0; i--) {
                            JSONObject valueObj = relEntityData.getJSONArray("valueList").getJSONObject(i);
                            if (StringUtils.isBlank(valueObj.getString("ciEntityName")) && valueObj.getLong("ciEntityId") != null) {
                                CiEntityVo cientity = ciEntityMapper.getCiEntityBaseInfoById(valueObj.getLong("ciEntityId"));
                                if (cientity != null) {
                                    valueObj.put("ciEntityName", cientity.getName());
                                } else {
                                    //relEntityData.getJSONArray("valueList").remove(i);
                                    valueObj.put("ciEntityName", $.t("新配置项"));
                                }
                            }
                        }
                    }
                }
            }
        }


        for (AttrVo attrVo : attrList) {
            if (!attrVo.getType().equals(EXPRESSION_TYPE)) {
                AttrEntityTransactionVo attrEntityTransactionVo = ciEntityTransactionVo.getAttrEntityTransactionByAttrId(attrVo.getId());
                /* 更新模式是全局模式时才做属性必填校验： */
                if (attrVo.getIsRequired().equals(1)) {
                    if (ciEntityTransactionVo.getEditMode().equals(EditModeType.GLOBAL.getValue())) {
                        if (attrEntityTransactionVo == null) {
                            if (!Objects.equals(attrVo.getAllowEdit(), 0)) {
                                throw new AttrEntityValueEmptyException(ciVo, attrVo);
                            }
                        } else if (attrEntityTransactionVo.getSaveMode().equals(SaveModeType.REPLACE.getValue()) && CollectionUtils.isEmpty(attrEntityTransactionVo.getValueList())) {
                            throw new AttrEntityValueEmptyException(ciVo, attrVo);
                        } else if (attrEntityTransactionVo.getSaveMode().equals(SaveModeType.MERGE.getValue()) && attrVo.isNeedTargetCi() && CollectionUtils.isEmpty(attrEntityTransactionVo.getValueList())) {
                            List<AttrEntityVo> oldList = ciEntityMapper.getAttrEntityByAttrIdAndFromCiEntityId(ciEntityTransactionVo.getCiEntityId(), attrVo.getId(), null);
                            if (CollectionUtils.isEmpty(oldList)) {
                                throw new AttrEntityValueEmptyException(ciVo, attrVo);
                            }
                        }
                    }
                }

                /*
                校验值是否符合数据类型
                 */
                if (attrEntityTransactionVo != null && CollectionUtils.isNotEmpty(attrEntityTransactionVo.getValueList()) && !attrVo.isNeedTargetCi()) {
                    IAttrValueHandler attrHandler = AttrValueHandlerFactory.getHandler(attrVo.getType());
                    if (attrHandler != null) {
                        attrHandler.valid(attrVo, attrEntityTransactionVo.getValueList());
                    } else {
                        throw new AttrTypeNotFoundException(attrVo.getType());
                    }
                }

                /*  调用校验器校验数据合法性，只有非引用型属性才需要 */
                if (attrEntityTransactionVo != null && CollectionUtils.isNotEmpty(attrEntityTransactionVo.getValueList()) && StringUtils.isNotBlank(attrVo.getValidatorHandler()) && !attrVo.isNeedTargetCi()) {
                    IValidator validator = ValidatorFactory.getValidator(attrVo.getValidatorHandler());
                    if (validator != null) {
                        validator.valid(attrVo, attrEntityTransactionVo.getValueList());
                    }
                }

                /* 检查属性是否唯一： */
                if (attrVo.getIsUnique().equals(1)) {
                    if (attrEntityTransactionVo != null && CollectionUtils.isNotEmpty(attrEntityTransactionVo.getValueList())) {
                        if (attrVo.isNeedTargetCi()) {
                            List<Long> toCiEntityIdList = new ArrayList<>();
                            for (int i = 0; i < attrEntityTransactionVo.getValueList().size(); i++) {
                                //如果不是id,则代表是新添加配置项，这时候不需要判断属性唯一
                                if (attrEntityTransactionVo.getValueList().get(i) instanceof Number) {
                                    Long tmpId = attrEntityTransactionVo.getValueList().getLong(i);
                                    if (tmpId != null) {
                                        toCiEntityIdList.add(tmpId);
                                    }
                                }
                            }
                            if (attrEntityTransactionVo.getSaveMode().equals(SaveModeType.MERGE.getValue())) {
                                //合并新老值
                                List<AttrEntityVo> oldList = ciEntityMapper.getAttrEntityByAttrIdAndFromCiEntityId(ciEntityTransactionVo.getCiEntityId(), attrVo.getId(), null);
                                for (AttrEntityVo attrEntityVo : oldList) {
                                    if (!toCiEntityIdList.contains(attrEntityVo.getToCiEntityId())) {
                                        toCiEntityIdList.add(attrEntityVo.getToCiEntityId());
                                    }
                                }
                            }
                            //检查新值是否被别的配置项引用
                            if (CollectionUtils.isNotEmpty(toCiEntityIdList)) {
                                int attrEntityCount = ciEntityMapper.getAttrEntityCountByAttrIdAndValue(ciEntityTransactionVo.getCiEntityId(), attrVo.getId(), toCiEntityIdList);
                                if (attrEntityCount > 0) {
                                    List<CiEntityVo> toCiEntityList = ciEntityMapper.getCiEntityBaseInfoByIdList(toCiEntityIdList);
                                    throw new AttrEntityDuplicateException(ciVo, attrVo.getLabel(), toCiEntityList.stream().map(CiEntityVo::getName).collect(Collectors.toList()));
                                }
                            }
                        } else {
                            //检查配置项表对应字段是否已被其他配置项使用
                            int count = ciEntityMapper.getCiEntityCountByAttrIdAndValue(ciEntityTransactionVo.getCiEntityId(), attrVo, attrEntityTransactionVo.getValueList().getString(0));
                            if (count > 0) {
                                throw new AttrEntityDuplicateException(ciVo, attrVo.getLabel(), attrEntityTransactionVo.getValueList());
                            }
                        }
                    }
                }

                //检查是否允许多选
                if (attrVo.getTargetCiId() != null && MapUtils.isNotEmpty(attrVo.getConfig()) && attrVo.getConfig().containsKey("isMultiple") && attrVo.getConfig().getString("isMultiple").equals("0")) {
                    if (attrEntityTransactionVo != null && CollectionUtils.isNotEmpty(attrEntityTransactionVo.getValueList()) && attrEntityTransactionVo.getValueList().size() > 1) {
                        throw new AttrEntityMultipleException(attrVo);
                    }
                }


            }
        }

        //所有需要删除的关系先记录到这里
        List<RelEntityVo> needDeleteRelEntityList = new ArrayList<>();
        // 校验关系信息
        for (RelVo relVo : relList) {
            // 判断当前配置项处于from位置的规则
            List<RelEntityTransactionVo> fromRelEntityTransactionList = ciEntityTransactionVo.getRelEntityTransactionByRelIdAndDirection(relVo.getId(), RelDirectionType.FROM.getValue());
            // 判断当前配置项处于to位置的规则
            List<RelEntityTransactionVo> toRelEntityTransactionList = ciEntityTransactionVo.getRelEntityTransactionByRelIdAndDirection(relVo.getId(), RelDirectionType.TO.getValue());

            // 标记当前模型是在关系的上端或者下端
            boolean isFrom = false;
            boolean isTo = false;
            if (relVo.getFromCiId().equals(ciEntityTransactionVo.getCiId())) {
                isFrom = true;
            }
            if (relVo.getToCiId().equals(ciEntityTransactionVo.getCiId())) {
                isTo = true;
            }
            // 全局模式下，不存在关系信息代表删除，需要校验必填规则
            if (ciEntityTransactionVo.getEditMode().equals(EditModeType.GLOBAL.getValue())) {
                if (CollectionUtils.isEmpty(fromRelEntityTransactionList)) {
                    if (isFrom && relVo.getToIsRequired().equals(1)) {
                        if (relVo.getAllowEdit() == null || relVo.getAllowEdit().equals(1)) {
                            throw new RelEntityNotFoundException(relVo.getToLabel());
                        }
                    }
                } else {
                    if (fromRelEntityTransactionList.size() > 1) {
                        // 检查关系是否允许重复
                        if (RelRuleType.O.getValue().equals(relVo.getToRule())) {
                            throw new RelEntityMultipleException(relVo.getToLabel());
                        }
                        if (relVo.getFromIsUnique().equals(1)) {
                            throw new RelEntityIsUsedException(RelDirectionType.FROM, relVo, false);
                        }
                    }
                    //检查关系唯一
                    if (relVo.getToIsUnique().equals(1)) {
                        for (RelEntityTransactionVo fromRelEntityVo : fromRelEntityTransactionList) {
                            List<RelEntityVo> checkFromRelEntityList = relEntityMapper.getRelEntityByToCiEntityIdAndRelId(fromRelEntityVo.getToCiEntityId(), relVo.getId(), null);
                            Optional<RelEntityVo> op = checkFromRelEntityList.stream().filter(r -> !r.getFromCiEntityId().equals(ciEntityTransactionVo.getCiEntityId())).findFirst();
                            if (op.isPresent()) {
                                throw new RelEntityIsUsedException(RelDirectionType.FROM, relVo, op.get());
                            }
                        }
                    }
                }

                // 全局模式下，不存在关系信息代表删除，需要校验必填规则
                if (CollectionUtils.isEmpty(toRelEntityTransactionList)) {
                    if (isTo && relVo.getFromIsRequired().equals(1)) {
                        if (relVo.getAllowEdit() == null || relVo.getAllowEdit().equals(1)) {
                            throw new RelEntityNotFoundException(relVo.getFromLabel());
                        }
                    }
                } else {
                    if (toRelEntityTransactionList.size() > 1) {
                        // 检查关系是否允许重复
                        if (RelRuleType.O.getValue().equals(relVo.getFromRule())) {
                            throw new RelEntityMultipleException(relVo.getFromLabel());
                        }
                        if (relVo.getToIsUnique().equals(1)) {
                            throw new RelEntityIsUsedException(RelDirectionType.TO, relVo, false);
                        }
                    }
                    //检查关系唯一
                    if (relVo.getFromIsUnique().equals(1)) {
                        for (RelEntityTransactionVo toRelEntityVo : toRelEntityTransactionList) {
                            List<RelEntityVo> checkFromRelEntityList = relEntityMapper.getRelEntityByFromCiEntityIdAndRelId(toRelEntityVo.getFromCiEntityId(), relVo.getId(), null);
                            Optional<RelEntityVo> op = checkFromRelEntityList.stream().filter(r -> !r.getToCiEntityId().equals(ciEntityTransactionVo.getCiEntityId())).findFirst();
                            if (op.isPresent()) {
                                throw new RelEntityIsUsedException(RelDirectionType.TO, relVo, op.get());
                            }
                        }
                    }
                }

            } else if (ciEntityTransactionVo.getEditMode().equals(EditModeType.PARTIAL.getValue())) {
                if (CollectionUtils.isNotEmpty(fromRelEntityTransactionList)) {
                    /*
                    检查是否有action=replace或update的关系，action=replace或update的数据来自自动发现同步，只要有一个是replace或update，所有都是replace或update，以下逻辑都是基于这个规则编写
                     */
                    boolean isReplace = fromRelEntityTransactionList.stream().anyMatch(d -> d.getAction().equals(RelActionType.REPLACE.getValue()));
                    boolean isUpdate = fromRelEntityTransactionList.stream().anyMatch(d -> d.getAction().equals(RelActionType.UPDATE.getValue()));
                    List<RelEntityVo> fromRelEntityList = null;
                    if (isReplace) {
                       /*
                       如果是replace模式，旧的关系数据需要清理掉，替换成新的关系数据,
                       replace还需要处理一种特殊场景，当关系为空，前面会组装一条残缺的数据进来，用于支持这里的逻辑进行replace操作，这里需要清除那条残缺数据
                        */
                        if (fromRelEntityTransactionList.size() == 1) {
                            if (fromRelEntityTransactionList.get(0).getToCiEntityId() == null) {
                                ciEntityTransactionVo.removeRelEntityData(relVo.getId(), RelDirectionType.FROM.getValue());
                                fromRelEntityTransactionList = new ArrayList<>();
                            }
                        }

                        fromRelEntityList = relEntityMapper.getRelEntityByFromCiEntityIdAndRelId(ciEntityTransactionVo.getCiEntityId(), relVo.getId(), null);
                        Iterator<RelEntityVo> fromRelEntityIt = fromRelEntityList.iterator();
                        while (fromRelEntityIt.hasNext()) {
                            RelEntityVo oldFromRelEntityVo = fromRelEntityIt.next();
                            /*
                            旧关系列表存在，新事务列表不存在的对象，先放到待删除列表里
                             */
                            if (fromRelEntityTransactionList.stream().noneMatch(d -> new RelEntityVo(d).equals(oldFromRelEntityVo))) {
                                needDeleteRelEntityList.add(oldFromRelEntityVo);
                                fromRelEntityIt.remove();
                            }
                        }
                    } else if (isUpdate) {
                        /*
                       如果是update模式，新关系为空时不做任何处理，新关系不为空时，替换成新的关系数据
                        */
                        if (CollectionUtils.isNotEmpty(fromRelEntityTransactionList)) {
                            fromRelEntityList = relEntityMapper.getRelEntityByFromCiEntityIdAndRelId(ciEntityTransactionVo.getCiEntityId(), relVo.getId(), null);
                            Iterator<RelEntityVo> fromRelEntityIt = fromRelEntityList.iterator();
                            while (fromRelEntityIt.hasNext()) {
                                RelEntityVo oldFromRelEntityVo = fromRelEntityIt.next();
                            /*
                            旧关系列表存在，新事务列表不存在的对象，先放到待删除列表里
                             */
                                if (fromRelEntityTransactionList.stream().noneMatch(d -> new RelEntityVo(d).equals(oldFromRelEntityVo))) {
                                    needDeleteRelEntityList.add(oldFromRelEntityVo);
                                    fromRelEntityIt.remove();
                                }
                            }
                        }
                    } else {
                        //筛选出非删除数据
                        fromRelEntityTransactionList = fromRelEntityTransactionList.stream().filter(d -> !Objects.equals(d.getAction(), RelActionType.DELETE.getValue())).collect(Collectors.toList());
                        //如果没有replace或update的数据，fromRelEntityList主要用来校验RelRuleType=O的规则，因此取两条数据即可
                        fromRelEntityList = relEntityMapper.getRelEntityByFromCiEntityIdAndRelId(ciEntityTransactionVo.getCiEntityId(), relVo.getId(), 2L);
                    }
                    if (CollectionUtils.isNotEmpty(fromRelEntityTransactionList)) {
                        if (fromRelEntityTransactionList.size() == 1 && CollectionUtils.isNotEmpty(fromRelEntityList)) {
                            // 检查关系是否允许重复
                            if (RelRuleType.O.getValue().equals(relVo.getToRule())) {
                                if ((fromRelEntityList.size() == 1 && !fromRelEntityList.contains(new RelEntityVo(fromRelEntityTransactionList.get(0)))) || fromRelEntityList.size() > 1) {
                                    throw new RelEntityMultipleException(relVo.getToLabel());
                                }
                            }
                        } else if (fromRelEntityTransactionList.size() > 1) {
                            // 检查关系是否允许重复
                            if (RelRuleType.O.getValue().equals(relVo.getToRule())) {
                                throw new RelEntityMultipleException(relVo.getToLabel());
                            }
                        }
                        //检查关系唯一
                        if (relVo.getToIsUnique().equals(1)) {
                            for (RelEntityTransactionVo fromRelEntityVo : fromRelEntityTransactionList) {
                                List<RelEntityVo> checkFromRelEntityList = relEntityMapper.getRelEntityByToCiEntityIdAndRelId(fromRelEntityVo.getToCiEntityId(), relVo.getId(), null);
                                Optional<RelEntityVo> op = checkFromRelEntityList.stream().filter(r -> !r.getFromCiEntityId().equals(ciEntityTransactionVo.getCiEntityId())).findFirst();
                                if (op.isPresent()) {
                                    throw new RelEntityIsUsedException(RelDirectionType.FROM, relVo, op.get());
                                }
                            }
                        }
                    }
                }


                if (CollectionUtils.isNotEmpty(toRelEntityTransactionList)) {
                     /*
                    检查是否有action=replace的关系，action=replace的数据来自自动发现同步，只要有一个是replace，所有都是replace，以下逻辑都是基于这个规则编写
                     */
                    boolean isReplace = toRelEntityTransactionList.stream().anyMatch(d -> d.getAction().equals(RelActionType.REPLACE.getValue()));
                    boolean isUpdate = toRelEntityTransactionList.stream().anyMatch(d -> d.getAction().equals(RelActionType.UPDATE.getValue()));

                    List<RelEntityVo> toRelEntityList = null;
                    if (isReplace) {
                         /*
                       如果是replace模式，旧的关系数据需要清理掉，替换成新的关系数据,
                       replace还需要处理一种特殊场景，当关系为空，前面会组装一条残缺的数据进来，用于支持这里的逻辑进行replace操作，这里需要清除那条残缺数据
                        */
                        if (toRelEntityTransactionList.size() == 1) {
                            if (toRelEntityTransactionList.get(0).getFromCiEntityId() == null) {
                                ciEntityTransactionVo.removeRelEntityData(relVo.getId(), RelDirectionType.TO.getValue());
                                toRelEntityTransactionList = new ArrayList<>();
                            }
                        }
                        toRelEntityList = relEntityMapper.getRelEntityByToCiEntityIdAndRelId(ciEntityTransactionVo.getCiEntityId(), relVo.getId(), null);
                        Iterator<RelEntityVo> toRelEntityIt = toRelEntityList.iterator();
                        while (toRelEntityIt.hasNext()) {
                            RelEntityVo oldToRelEntityVo = toRelEntityIt.next();
                            /*
                            旧关系列表不存在事务列表的对象先放到待删除列表里
                             */
                            if (toRelEntityTransactionList.stream().noneMatch(d -> new RelEntityVo(d).equals(oldToRelEntityVo))) {
                                needDeleteRelEntityList.add(oldToRelEntityVo);
                                toRelEntityIt.remove();
                            }
                        }
                    } else if (isUpdate) {
                        /*
                       如果是update模式，新关系为空时不做任何处理，新关系不为空时，替换成新的关系数据
                        */
                        if (CollectionUtils.isNotEmpty(toRelEntityTransactionList)) {
                            toRelEntityList = relEntityMapper.getRelEntityByToCiEntityIdAndRelId(ciEntityTransactionVo.getCiEntityId(), relVo.getId(), null);
                            Iterator<RelEntityVo> toRelEntityIt = toRelEntityList.iterator();
                            while (toRelEntityIt.hasNext()) {
                                RelEntityVo oldToRelEntityVo = toRelEntityIt.next();
                            /*
                            旧关系列表不存在事务列表的对象先放到待删除列表里
                             */
                                if (toRelEntityTransactionList.stream().noneMatch(d -> new RelEntityVo(d).equals(oldToRelEntityVo))) {
                                    needDeleteRelEntityList.add(oldToRelEntityVo);
                                    toRelEntityIt.remove();
                                }
                            }
                        }
                    } else {
                        //筛选出非删除数据
                        toRelEntityTransactionList = toRelEntityTransactionList.stream().filter(d -> !Objects.equals(d.getAction(), RelActionType.DELETE.getValue())).collect(Collectors.toList());
                        //如果没有replace或update的数据，fromRelEntityList主要用来校验RelRuleType=O的规则，因此取两条数据即可
                        toRelEntityList = relEntityMapper.getRelEntityByToCiEntityIdAndRelId(ciEntityTransactionVo.getCiEntityId(), relVo.getId(), 2L);

                    }
                    if (toRelEntityTransactionList.size() == 1) {
                        // 检查关系是否允许重复
                        if (RelRuleType.O.getValue().equals(relVo.getFromRule()) && CollectionUtils.isNotEmpty(toRelEntityList)) {
                            if ((toRelEntityList.size() == 1 && !toRelEntityList.contains(new RelEntityVo(toRelEntityTransactionList.get(0)))) || toRelEntityList.size() > 1) {
                                throw new RelEntityMultipleException(relVo.getFromLabel());
                            }
                        }
                    } else if (fromRelEntityTransactionList.size() > 1) {
                        // 检查关系是否允许重复
                        if (RelRuleType.O.getValue().equals(relVo.getFromRule())) {
                            throw new RelEntityMultipleException(relVo.getFromLabel());
                        }
                    }
                    //检查关系唯一
                    if (relVo.getFromIsUnique().equals(1)) {
                        for (RelEntityTransactionVo toRelEntityVo : toRelEntityTransactionList) {
                            List<RelEntityVo> checkFromRelEntityList = relEntityMapper.getRelEntityByFromCiEntityIdAndRelId(toRelEntityVo.getFromCiEntityId(), relVo.getId(), null);
                            Optional<RelEntityVo> op = checkFromRelEntityList.stream().filter(r -> !r.getToCiEntityId().equals(ciEntityTransactionVo.getCiEntityId())).findFirst();
                            if (op.isPresent()) {
                                throw new RelEntityIsUsedException(RelDirectionType.TO, relVo, op.get());
                            }
                        }
                    }
                }
            }
        }

        //校验唯一规则
        if (CollectionUtils.isNotEmpty(ciVo.getUniqueAttrIdList())) {
            List<String> valueList = new ArrayList<>();
            CiEntityVo ciEntityConditionVo = new CiEntityVo();
            ciEntityConditionVo.setCiId(ciEntityTransactionVo.getCiId());
            for (Long attrId : ciVo.getUniqueAttrIdList()) {
                Optional<AttrVo> op = attrList.stream().filter(d -> d.getId().equals(attrId)).findFirst();
                if (!op.isPresent()) {
                    throw new AttrNotFoundException(attrId);
                }
                AttrEntityTransactionVo attrEntityTransactionVo = ciEntityTransactionVo.getAttrEntityTransactionByAttrId(attrId);
                if (attrEntityTransactionVo != null) {
                    AttrFilterVo filterVo = new AttrFilterVo();
                    filterVo.setAttrId(attrId);
                    filterVo.setExpression(SearchExpression.EQ.getExpression());
                    filterVo.setValueList(attrEntityTransactionVo.getValueList().stream().map(d -> {
                        if (d != null) {
                            if (StringUtils.isBlank(d.toString())) {
                                throw new CiUniqueAttrNotFoundException(op.get());
                            }
                            return d.toString();
                        } else {
                            throw new CiUniqueAttrNotFoundException(op.get());
                        }
                    }).collect(Collectors.toList()));
                    valueList.add(String.join(",", filterVo.getValueList()));
                    ciEntityConditionVo.addAttrFilter(filterVo);
                } else {
                    if (oldEntity != null) {
                        AttrEntityVo attrEntityVo = oldEntity.getAttrEntityByAttrId(attrId);
                        if (attrEntityVo != null && CollectionUtils.isNotEmpty(attrEntityVo.getValueList())) {
                            AttrFilterVo filterVo = new AttrFilterVo();
                            filterVo.setAttrId(attrId);
                            filterVo.setExpression(SearchExpression.EQ.getExpression());
                            filterVo.setValueList(attrEntityVo.getValueList().stream().map(d -> {
                                if (d != null) {
                                    if (StringUtils.isBlank(d.toString())) {
                                        throw new CiUniqueAttrNotFoundException(op.get());
                                    }
                                    return d.toString();
                                } else {
                                    throw new CiUniqueAttrNotFoundException(op.get());
                                }
                            }).collect(Collectors.toList()));
                            valueList.add(String.join(",", filterVo.getValueList()));
                            ciEntityConditionVo.addAttrFilter(filterVo);
                        } else {
                            throw new CiUniqueAttrNotFoundException(op.get());
                        }
                    } else {
                        //没有oldEntity代表新添加数据，如果没有唯一属性值则需要抛异常
                        throw new CiUniqueAttrNotFoundException(op.get());
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(ciEntityConditionVo.getAttrFilterList())) {
                //检查是否存在不需要join所有属性和关系
                List<Long> noIdList = new ArrayList<>();
                noIdList.add(0L);
                ciEntityConditionVo.setAttrIdList(noIdList);
                ciEntityConditionVo.setRelIdList(noIdList);
                List<CiEntityVo> checkList = this.searchCiEntity(ciEntityConditionVo);
                for (CiEntityVo checkCiEntity : checkList) {
                    if (!checkCiEntity.getId().equals(ciEntityTransactionVo.getCiEntityId())) {
                        //如果是更新，并且设置了跳过唯一规则校验，才忽略唯一规则报错
                        if (!ciEntityTransactionVo.getAction().equals(TransactionActionType.UPDATE.getValue()) || !ciEntityTransactionVo.getSkipUniqueCheck()) {
                            throw new CiUniqueRuleException(ciVo, String.join(",", valueList));
                        }
                    }
                }
            }
        }

        // 去掉没变化的属性修改
        boolean hasChange = false;
        if (CollectionUtils.isNotEmpty(ciEntityTransactionVo.getAttrEntityTransactionList())) {
            if (CollectionUtils.isNotEmpty(oldAttrEntityList)) {
                List<AttrEntityTransactionVo> oldAttrEntityTransactionList = oldAttrEntityList.stream().map(AttrEntityTransactionVo::new).collect(Collectors.toList());
                // 去掉没变化的修改
                ciEntityTransactionVo.removeAttrEntityData(oldAttrEntityTransactionList);
                if (MapUtils.isNotEmpty(ciEntityTransactionVo.getAttrEntityData())) {
                    hasChange = true;
                }
            } else {
                hasChange = true;
            }
        }
        //去掉没有变化全局属性修改
        if (CollectionUtils.isNotEmpty(ciEntityTransactionVo.getGlobalAttrTransactionList())) {
            if (CollectionUtils.isNotEmpty(oldGlobalAttrEntityList)) {
                List<GlobalAttrEntityTransactionVo> oldGlobalAttrTransactionList = oldGlobalAttrEntityList.stream().map(GlobalAttrEntityTransactionVo::new).collect(Collectors.toList());
                // 去掉没变化的修改
                ciEntityTransactionVo.removeGlobalAttrEntityData(oldGlobalAttrTransactionList);
                if (MapUtils.isNotEmpty(ciEntityTransactionVo.getGlobalAttrEntityData())) {
                    hasChange = true;
                }
            } else {
                hasChange = true;
            }
        }

        // List<RelEntityTransactionVo> newRelEntityTransactionList = ciEntityTransactionVo.getRelEntityTransactionList();


        // 全局修改模式下，事务中不包含的关系代表要删除
        if (ciEntityTransactionVo.getEditMode().equals(EditModeType.GLOBAL.getValue())) {
            if (CollectionUtils.isNotEmpty(oldRelEntityList)) {
                //找到旧对象中存在，但在事务中不存在的关系，重新添加到事务中，并且把操作设为删除
                for (RelEntityVo relEntityVo : oldRelEntityList) {
                    Long targetId = relEntityVo.getDirection().equals(RelDirectionType.FROM.getValue()) ? relEntityVo.getToCiEntityId() : relEntityVo.getFromCiEntityId();
                    if (!ciEntityTransactionVo.containRelEntityData(relEntityVo.getRelId(), relEntityVo.getDirection(), targetId)) {
                        //先暂存在待删除列表里，等清除完没变化的关系后再添加到事务里
                        needDeleteRelEntityList.add(relEntityVo);
                    }
                }
            }
        }

        // 排除掉没变化的关系
        if (CollectionUtils.isNotEmpty(oldRelEntityList)) {
            for (RelEntityVo relEntityVo : oldRelEntityList) {
                Long targetId = relEntityVo.getDirection().equals(RelDirectionType.FROM.getValue()) ? relEntityVo.getToCiEntityId() : relEntityVo.getFromCiEntityId();
                ciEntityTransactionVo.removeRelEntityData(relEntityVo.getRelId(), relEntityVo.getDirection(), targetId);
            }
        }

        //补充待删除关系进事务里
        for (RelEntityVo relEntityVo : needDeleteRelEntityList) {
            Long targetCiId;
            Long targetCiEntityId;
            String targetCiEntityName;
            if (relEntityVo.getDirection().equals(RelDirectionType.FROM.getValue())) {
                targetCiEntityId = relEntityVo.getToCiEntityId();
                targetCiId = relEntityVo.getToCiId();
                targetCiEntityName = relEntityVo.getToCiEntityName();
            } else {
                targetCiEntityId = relEntityVo.getFromCiEntityId();
                targetCiId = relEntityVo.getFromCiId();
                targetCiEntityName = relEntityVo.getFromCiEntityName();
            }
            RelVo relVo = relMapper.getRelById(relEntityVo.getRelId());
            ciEntityTransactionVo.addRelEntityData(relVo, relEntityVo.getDirection(), targetCiId, targetCiEntityId, targetCiEntityName, RelActionType.DELETE.getValue());
        }

        if (MapUtils.isNotEmpty(ciEntityTransactionVo.getRelEntityData())) {
            hasChange = true;
        } else {
            //如果没有任何修改，也要renew一下配置项和关系
            ciEntityMapper.updateCiEntityBaseInfo(new CiEntityVo(ciEntityTransactionVo.getCiEntityId()));

        }
        return hasChange;
    }


    /**
     * 验证事务是否能提交
     * 1、如果是删除，直接通过。
     * 2、如果是修改，判断配置项是否已经被删除。
     * <p>
     * 属性必填校验：
     * 1、删除的属性才需要校验属性是否必填。
     * <p>
     * 检查属性是否唯一：
     * 1、如果是引用值，则存在多值的可能性，需要根据保存模式来组装数据进行校验。
     * 如果是replace模式，直接用新值进行校验。
     * 如果是Merge模式，则需要先把新值和旧值合并后再进行校验。
     * 2、如果是普通属性值，只有单值的可能，不管什么模式只需要使用新值进行校验即可。
     * <p>
     * 检查唯一规则：
     * 1、如果唯一规则包含引用值，
     * <p>
     * 记录属性是新增、编辑或删除：
     * 1、如果整个attr_xxxx在snapshot中不存在，代表添加了新属性，如果attr_xxxx的valueList在snapshot中为空，代表属性添加了值。
     * 2、如果snapshot本身已存在attr_xxx和valueList，代表编辑。
     * 3、如果修改模式是Partial，attr_xxx为空，代表删除整个属性，如果只是valueList清空，代表属性删除了值，其他不提供的代表不动。
     *
     * @param ciEntityTransactionVo 配置项事务实体
     * @return true:验证成功 false:验证失败
     */
    private boolean validateCiEntityTransactionForCommit(CiEntityTransactionVo ciEntityTransactionVo) {
        if (ciEntityTransactionVo.getAction().equals(TransactionActionType.DELETE.getValue())) {
            return true;
        } else {
            List<AttrVo> attrList = attrMapper.getAttrByCiId(ciEntityTransactionVo.getCiId());
            List<RelVo> relList = RelUtil.ClearRepeatRel(relMapper.getRelByCiId(ciEntityTransactionVo.getCiId()));
            CiVo ciVo = ciMapper.getCiById(ciEntityTransactionVo.getCiId());

            // 清除不存在的属性
            if (MapUtils.isNotEmpty(ciEntityTransactionVo.getAttrEntityData())) {
                List<AttrEntityTransactionVo> attrEntityList = ciEntityTransactionVo.getAttrEntityTransactionList();
                for (AttrEntityTransactionVo attrEntityTransaction : attrEntityList) {
                    if (attrList.stream().noneMatch(a -> a.getId().equals(attrEntityTransaction.getAttrId()))) {
                        ciEntityTransactionVo.removeAttrEntityData(attrEntityTransaction.getAttrId());
                    }
                }
            }

            // 消除不存在的关系
            if (MapUtils.isNotEmpty(ciEntityTransactionVo.getRelEntityData())) {
                List<RelEntityTransactionVo> relEntityList = ciEntityTransactionVo.getRelEntityTransactionList();
                for (RelEntityTransactionVo relEntity : relEntityList) {
                    if (relList.stream().noneMatch(r -> r.getId().equals(relEntity.getRelId()) && r.getDirection().equals(relEntity.getDirection()))) {
                        ciEntityTransactionVo.removeRelEntityData(relEntity.getRelId(), relEntity.getDirection());
                    }
                }
            }

            if (ciEntityTransactionVo.getAction().equals(TransactionActionType.INSERT.getValue()) || ciEntityTransactionVo.getAction().equals(TransactionActionType.RECOVER.getValue())) {
                //新增配置项需要全面校验属性
                for (AttrVo attrVo : attrList) {
                    if (!attrVo.getType().equals(EXPRESSION_TYPE)) {
                        AttrEntityTransactionVo attrEntityTransactionVo = ciEntityTransactionVo.getAttrEntityTransactionByAttrId(attrVo.getId());
                        /* 属性必填校验： */
                        if (attrVo.getIsRequired().equals(1)) {
                            if (attrEntityTransactionVo == null || CollectionUtils.isEmpty(attrEntityTransactionVo.getValueList())) {
                                if (attrVo.getAllowEdit() == null || attrVo.getAllowEdit().equals(1)) {
                                    throw new AttrEntityValueEmptyException(ciVo, attrVo);
                                }
                            }
                        }

                        /* 校验值是否符合数据类型*/
                        if (attrEntityTransactionVo != null && CollectionUtils.isNotEmpty(attrEntityTransactionVo.getValueList()) && Boolean.FALSE.equals(attrVo.isNeedTargetCi())) {
                            IAttrValueHandler attrHandler = AttrValueHandlerFactory.getHandler(attrVo.getType());
                            if (attrHandler != null) {
                                attrHandler.valid(attrVo, attrEntityTransactionVo.getValueList());
                            } else {
                                throw new AttrTypeNotFoundException(attrVo.getType());
                            }
                        }

                        /*  调用校验器校验数据合法性，只有非引用型属性才需要 */
                        if (attrEntityTransactionVo != null && CollectionUtils.isNotEmpty(attrEntityTransactionVo.getValueList()) && StringUtils.isNotBlank(attrVo.getValidatorHandler()) && Boolean.FALSE.equals(attrVo.isNeedTargetCi())) {
                            IValidator validator = ValidatorFactory.getValidator(attrVo.getValidatorHandler());
                            if (validator != null) {
                                validator.valid(attrVo, attrEntityTransactionVo.getValueList());
                            }
                        }

                        /* 检查属性是否唯一： */
                        if (attrVo.getIsUnique().equals(1)) {
                            if (attrEntityTransactionVo != null && CollectionUtils.isNotEmpty(attrEntityTransactionVo.getValueList())) {
                                if (attrVo.isNeedTargetCi()) {
                                    List<Long> toCiEntityIdList = new ArrayList<>();
                                    for (int i = 0; i < attrEntityTransactionVo.getValueList().size(); i++) {
                                        if (attrEntityTransactionVo.getValueList().get(i) instanceof Number) {
                                            Long tmpId = attrEntityTransactionVo.getValueList().getLong(i);
                                            if (tmpId != null) {
                                                toCiEntityIdList.add(tmpId);
                                            }
                                        }
                                    }
                                    //检查新值是否被别的配置项引用
                                    if (CollectionUtils.isNotEmpty(toCiEntityIdList)) {
                                        int attrEntityCount = ciEntityMapper.getAttrEntityCountByAttrIdAndValue(ciEntityTransactionVo.getCiEntityId(), attrVo.getId(), toCiEntityIdList);
                                        if (attrEntityCount > 0) {
                                            List<CiEntityVo> toCiEntityList = ciEntityMapper.getCiEntityBaseInfoByIdList(toCiEntityIdList);
                                            throw new AttrEntityDuplicateException(ciVo, attrVo.getLabel(), toCiEntityList.stream().map(CiEntityVo::getName).collect(Collectors.toList()));
                                        }
                                    }
                                } else {
                                    //检查配置项表对应字段是否已被其他配置项使用
                                    int count = ciEntityMapper.getCiEntityCountByAttrIdAndValue(ciEntityTransactionVo.getCiEntityId(), attrVo, attrEntityTransactionVo.getValueList().getString(0));
                                    if (count > 0) {
                                        throw new AttrEntityDuplicateException(ciVo, attrVo.getLabel(), attrEntityTransactionVo.getValueList());
                                    }
                                }
                            }
                        }


                    }
                }

                // 校验关系信息
                for (RelVo relVo : relList) {
                    // 判断当前配置项处于from位置的规则
                    List<RelEntityTransactionVo> fromRelEntityTransactionList = ciEntityTransactionVo.getRelEntityTransactionByRelIdAndDirection(relVo.getId(), RelDirectionType.FROM.getValue());
                    // 判断当前配置项处于to位置的规则
                    List<RelEntityTransactionVo> toRelEntityTransactionList = ciEntityTransactionVo.getRelEntityTransactionByRelIdAndDirection(relVo.getId(), RelDirectionType.TO.getValue());

                    // 标记当前模型是在关系的上端或者下端
                    boolean isFrom = false;
                    boolean isTo = false;
                    if (relVo.getFromCiId().equals(ciEntityTransactionVo.getCiId())) {
                        isFrom = true;
                    }
                    if (relVo.getToCiId().equals(ciEntityTransactionVo.getCiId())) {
                        isTo = true;
                    }


                    if (CollectionUtils.isEmpty(fromRelEntityTransactionList)) {
                        if (isFrom && relVo.getToIsRequired().equals(1)) {
                            if (relVo.getAllowEdit() == null || relVo.getAllowEdit().equals(1)) {
                                throw new RelEntityNotFoundException(relVo.getToLabel());
                            }
                        }
                    } else if (fromRelEntityTransactionList.size() > 1) {
                        // 检查关系是否允许重复
                        if (RelRuleType.O.getValue().equals(relVo.getToRule())) {
                            throw new RelEntityMultipleException(relVo.getToLabel());
                        }
                    }

                    if (CollectionUtils.isEmpty(toRelEntityTransactionList)) {
                        if (isTo && relVo.getFromIsRequired().equals(1)) {
                            if (relVo.getAllowEdit() == null || relVo.getAllowEdit().equals(1)) {
                                throw new RelEntityNotFoundException(relVo.getFromLabel());
                            }
                        }
                    } else if (toRelEntityTransactionList.size() > 1) {
                        // 检查关系是否允许重复
                        if (RelRuleType.O.getValue().equals(relVo.getFromRule())) {
                            throw new RelEntityMultipleException(relVo.getFromLabel());
                        }
                    }
                }
                //校验唯一规则
                if (CollectionUtils.isNotEmpty(ciVo.getUniqueAttrIdList())) {
                    CiEntityVo ciEntityConditionVo = new CiEntityVo();
                    ciEntityConditionVo.setCiId(ciEntityTransactionVo.getCiId());
                    List<Long> emptyList = new ArrayList<>();
                    emptyList.add(0L);
                    ciEntityConditionVo.setAttrIdList(emptyList);
                    ciEntityConditionVo.setRelIdList(emptyList);
                    for (Long attrId : ciVo.getUniqueAttrIdList()) {
                        AttrEntityTransactionVo attrEntityTransactionVo = ciEntityTransactionVo.getAttrEntityTransactionByAttrId(attrId);
                        if (attrEntityTransactionVo != null) {
                            AttrFilterVo filterVo = new AttrFilterVo();
                            filterVo.setAttrId(attrId);
                            filterVo.setExpression(SearchExpression.EQ.getExpression());
                            filterVo.setValueList(attrEntityTransactionVo.getValueList().stream().map(Object::toString).collect(Collectors.toList()));
                            ciEntityConditionVo.addAttrFilter(filterVo);
                        }
                    }
                    if (CollectionUtils.isNotEmpty(ciEntityConditionVo.getAttrFilterList())) {
                        List<CiEntityVo> checkList = this.searchCiEntity(ciEntityConditionVo);
                        for (CiEntityVo checkCiEntity : checkList) {
                            if (!checkCiEntity.getId().equals(ciEntityTransactionVo.getCiEntityId())) {
                                throw new CiUniqueRuleException(ciVo);
                            }
                        }
                    }
                }
            } else if (ciEntityTransactionVo.getAction().equals(TransactionActionType.UPDATE.getValue())) {
                CiEntityVo oldEntity = this.getCiEntityByIdLite(ciEntityTransactionVo.getCiId(), ciEntityTransactionVo.getCiEntityId(), true, false, false);
                if (oldEntity == null) {
                    throw new CiEntityNotFoundException(ciEntityTransactionVo.getCiEntityId());
                }
                for (AttrVo attrVo : attrList) {
                    if (!attrVo.getType().equals(EXPRESSION_TYPE)) {
                        AttrEntityTransactionVo attrEntityTransactionVo = ciEntityTransactionVo.getAttrEntityTransactionByAttrId(attrVo.getId());
                        if (attrEntityTransactionVo != null) {
                            //属性是否需要删除
                            boolean isDelete = CollectionUtils.isEmpty(attrEntityTransactionVo.getValueList()) || (attrEntityTransactionVo.getValueList().size() == 1 && StringUtils.isBlank(attrEntityTransactionVo.getValueList().getString(0)));
                            /*检查必填属性：*/
                            if (attrVo.getIsRequired().equals(1) && isDelete) {
                                throw new AttrEntityValueEmptyException(ciVo, attrVo);
                            }

                            /* 校验值是否符合数据类型*/
                            if (CollectionUtils.isNotEmpty(attrEntityTransactionVo.getValueList()) && !attrVo.isNeedTargetCi()) {
                                IAttrValueHandler attrHandler = AttrValueHandlerFactory.getHandler(attrVo.getType());
                                if (attrHandler != null) {
                                    attrHandler.valid(attrVo, attrEntityTransactionVo.getValueList());
                                } else {
                                    throw new AttrTypeNotFoundException(attrVo.getType());
                                }
                            }

                            /*  调用校验器校验数据合法性，只有非引用型属性才需要 */
                            if (CollectionUtils.isNotEmpty(attrEntityTransactionVo.getValueList()) && StringUtils.isNotBlank(attrVo.getValidatorHandler()) && !attrVo.isNeedTargetCi()) {
                                IValidator validator = ValidatorFactory.getValidator(attrVo.getValidatorHandler());
                                if (validator != null) {
                                    validator.valid(attrVo, attrEntityTransactionVo.getValueList());
                                }
                            }

                            /* 检查属性是否唯一： */
                            if (!isDelete && attrVo.getIsUnique().equals(1)) {
                                if (attrVo.isNeedTargetCi()) {
                                    List<Long> toCiEntityIdList = new ArrayList<>();
                                    for (int i = 0; i < attrEntityTransactionVo.getValueList().size(); i++) {
                                        Long tmpId = attrEntityTransactionVo.getValueList().getLong(i);
                                        if (tmpId != null) {
                                            toCiEntityIdList.add(tmpId);
                                        }
                                    }
                                    if (attrEntityTransactionVo.getSaveMode().equals(SaveModeType.MERGE.getValue())) {
                                        //合并新老值
                                        List<AttrEntityVo> oldList = ciEntityMapper.getAttrEntityByAttrIdAndFromCiEntityId(ciEntityTransactionVo.getCiEntityId(), attrVo.getId(), null);
                                        for (AttrEntityVo attrEntityVo : oldList) {
                                            if (!toCiEntityIdList.contains(attrEntityVo.getToCiEntityId())) {
                                                toCiEntityIdList.add(attrEntityVo.getToCiEntityId());
                                            }
                                        }
                                    }
                                    //检查新值是否被别的配置项引用
                                    int attrEntityCount = ciEntityMapper.getAttrEntityCountByAttrIdAndValue(ciEntityTransactionVo.getCiEntityId(), attrVo.getId(), toCiEntityIdList);
                                    if (attrEntityCount > 0) {
                                        List<CiEntityVo> toCiEntityList = ciEntityMapper.getCiEntityBaseInfoByIdList(toCiEntityIdList);
                                        throw new AttrEntityDuplicateException(ciVo, attrVo.getLabel(), toCiEntityList.stream().map(CiEntityVo::getName).collect(Collectors.toList()));
                                    }
                                } else {
                                    //检查配置项表对应字段是否已被其他配置项使用
                                    int count = ciEntityMapper.getCiEntityCountByAttrIdAndValue(ciEntityTransactionVo.getCiEntityId(), attrVo, attrEntityTransactionVo.getValueList().getString(0));
                                    if (count > 0) {
                                        throw new AttrEntityDuplicateException(ciVo, attrVo.getLabel(), attrEntityTransactionVo.getValueList());
                                    }
                                }
                            }


                        }
                    }
                }

                // 校验关系信息
                for (RelVo relVo : relList) {
                    // 判断当前配置项处于from位置的规则
                    List<RelEntityTransactionVo> fromRelEntityTransactionList = ciEntityTransactionVo.getRelEntityTransactionByRelIdAndDirection(relVo.getId(), RelDirectionType.FROM.getValue());
                    // 判断当前配置项处于to位置的规则
                    List<RelEntityTransactionVo> toRelEntityTransactionList = ciEntityTransactionVo.getRelEntityTransactionByRelIdAndDirection(relVo.getId(), RelDirectionType.TO.getValue());

                    // 标记当前模型是在关系的上端或者下端
                    boolean isFrom = false;
                    boolean isTo = false;
                    if (relVo.getFromCiId().equals(ciEntityTransactionVo.getCiId())) {
                        isFrom = true;
                    }
                    if (relVo.getToCiId().equals(ciEntityTransactionVo.getCiId())) {
                        isTo = true;
                    }


                    if (CollectionUtils.isNotEmpty(fromRelEntityTransactionList)) {
                        //判断关系必填
                        if (isFrom && relVo.getToIsRequired().equals(1)) {
                            List<RelEntityTransactionVo> delEntityList = fromRelEntityTransactionList.stream().filter(r -> r.getAction().equals(RelActionType.DELETE.getValue())).collect(Collectors.toList());
                            if (CollectionUtils.isNotEmpty(delEntityList)) {
                                //如果关系早已经被清空，则不需要再做必填校验
                                if (CollectionUtils.isNotEmpty(oldEntity.getRelEntityByRelIdAndDirection(relVo.getId(), RelDirectionType.FROM.getValue()))) {
                                    for (RelEntityTransactionVo relEntity : delEntityList) {
                                        oldEntity.removeRelEntityData(relEntity.getRelId(), RelDirectionType.FROM.getValue(), relEntity.getToCiEntityId());
                                    }
                                }
                                if (CollectionUtils.isEmpty(oldEntity.getRelEntityByRelIdAndDirection(relVo.getId(), RelDirectionType.FROM.getValue()))) {
                                    throw new RelEntityNotFoundException(relVo.getToLabel());
                                }
                            }
                        }
                        //判断关系多选
                        if (RelRuleType.O.getValue().equals(relVo.getToRule())) {
                            List<RelEntityTransactionVo> insertEntityList = fromRelEntityTransactionList.stream().filter(r -> r.getAction().equals(RelActionType.INSERT.getValue())).collect(Collectors.toList());
                            if (CollectionUtils.isNotEmpty(insertEntityList)) {
                                if (insertEntityList.size() > 1) {
                                    throw new RelEntityMultipleException(relVo.getToLabel());
                                } else {
                                    if (CollectionUtils.isNotEmpty(oldEntity.getRelEntityByRelIdAndDirection(relVo.getId(), RelDirectionType.FROM.getValue()))) {
                                        for (RelEntityTransactionVo relEntity : insertEntityList) {
                                            oldEntity.removeRelEntityData(relEntity.getRelId(), RelDirectionType.FROM.getValue(), relEntity.getToCiEntityId());
                                        }
                                    }
                                    if (CollectionUtils.isNotEmpty(oldEntity.getRelEntityByRelIdAndDirection(relVo.getId(), RelDirectionType.FROM.getValue()))) {
                                        throw new RelEntityMultipleException(relVo.getToLabel());
                                    }
                                }
                            }
                        }
                        //判断关系唯一
                        if (relVo.getToIsUnique().equals(1)) {
                            for (RelEntityTransactionVo fromRelEntityVo : fromRelEntityTransactionList) {
                                List<RelEntityVo> checkFromRelEntityList = relEntityMapper.getRelEntityByToCiEntityIdAndRelId(fromRelEntityVo.getToCiEntityId(), relVo.getId(), null);
                                Optional<RelEntityVo> op = checkFromRelEntityList.stream().filter(r -> !r.getFromCiEntityId().equals(ciEntityTransactionVo.getCiEntityId())).findFirst();
                                if (op.isPresent()) {
                                    throw new RelEntityIsUsedException(RelDirectionType.FROM, relVo, op.get());
                                }
                            }
                        }
                    }

                    if (CollectionUtils.isNotEmpty(toRelEntityTransactionList)) {
                        //判断关系必填
                        if (isTo && relVo.getFromIsRequired().equals(1)) {
                            List<RelEntityTransactionVo> delEntityList = toRelEntityTransactionList.stream().filter(r -> r.getAction().equals(RelActionType.DELETE.getValue())).collect(Collectors.toList());
                            if (CollectionUtils.isNotEmpty(delEntityList)) {
                                //如果关系早已经被清空，则不需要再做必填校验
                                if (CollectionUtils.isNotEmpty(oldEntity.getRelEntityByRelIdAndDirection(relVo.getId(), RelDirectionType.TO.getValue()))) {
                                    for (RelEntityTransactionVo relEntity : delEntityList) {
                                        oldEntity.removeRelEntityData(relEntity.getRelId(), RelDirectionType.TO.getValue(), relEntity.getFromCiEntityId());
                                    }
                                }
                                if (CollectionUtils.isEmpty(oldEntity.getRelEntityByRelIdAndDirection(relVo.getId(), RelDirectionType.TO.getValue()))) {
                                    throw new RelEntityNotFoundException(relVo.getFromLabel());
                                }
                            }
                        }

                        //判断关系多选
                        if (RelRuleType.O.getValue().equals(relVo.getFromRule())) {
                            List<RelEntityTransactionVo> insertEntityList = toRelEntityTransactionList.stream().filter(r -> r.getAction().equals(RelActionType.INSERT.getValue())).collect(Collectors.toList());
                            if (CollectionUtils.isNotEmpty(insertEntityList)) {
                                if (insertEntityList.size() > 1) {
                                    throw new RelEntityMultipleException(relVo.getFromLabel());
                                } else {
                                    if (CollectionUtils.isNotEmpty(oldEntity.getRelEntityByRelIdAndDirection(relVo.getId(), RelDirectionType.TO.getValue()))) {
                                        for (RelEntityTransactionVo relEntity : insertEntityList) {
                                            oldEntity.removeRelEntityData(relEntity.getRelId(), RelDirectionType.TO.getValue(), relEntity.getFromCiEntityId());
                                        }
                                    }
                                    if (CollectionUtils.isNotEmpty(oldEntity.getRelEntityByRelIdAndDirection(relVo.getId(), RelDirectionType.TO.getValue()))) {
                                        throw new RelEntityMultipleException(relVo.getToLabel());
                                    }
                                }
                            }
                        }
                        //判断关系唯一
                        if (relVo.getFromIsUnique().equals(1)) {
                            for (RelEntityTransactionVo toRelEntityVo : toRelEntityTransactionList) {
                                List<RelEntityVo> checkFromRelEntityList = relEntityMapper.getRelEntityByFromCiEntityIdAndRelId(toRelEntityVo.getFromCiEntityId(), relVo.getId(), null);
                                Optional<RelEntityVo> op = checkFromRelEntityList.stream().filter(r -> !r.getToCiEntityId().equals(ciEntityTransactionVo.getCiEntityId())).findFirst();
                                if (op.isPresent()) {
                                    throw new RelEntityIsUsedException(RelDirectionType.TO, relVo, op.get());
                                }
                            }
                        }
                    }
                }

                //校验唯一规则
                if (CollectionUtils.isNotEmpty(ciVo.getUniqueAttrIdList())) {
                    CiEntityVo ciEntityConditionVo = new CiEntityVo();
                    ciEntityConditionVo.setCiId(ciEntityTransactionVo.getCiId());
                    ciEntityConditionVo.setAttrIdList(new ArrayList<Long>() {{
                        this.add(0L);
                    }});
                    ciEntityConditionVo.setRelIdList(new ArrayList<Long>() {{
                        this.add(0L);
                    }});
                    for (Long attrId : ciVo.getUniqueAttrIdList()) {
                        AttrEntityTransactionVo attrEntityTransactionVo = ciEntityTransactionVo.getAttrEntityTransactionByAttrId(attrId);
                        if (attrEntityTransactionVo != null) {
                            AttrFilterVo filterVo = new AttrFilterVo();
                            filterVo.setAttrId(attrId);
                            filterVo.setExpression(SearchExpression.EQ.getExpression());
                            filterVo.setValueList(attrEntityTransactionVo.getValueList().stream().map(Object::toString).collect(Collectors.toList()));
                            ciEntityConditionVo.addAttrFilter(filterVo);
                        } else {
                            AttrEntityVo attrEntityVo = oldEntity.getAttrEntityByAttrId(attrId);
                            if (attrEntityVo != null) {
                                AttrFilterVo filterVo = new AttrFilterVo();
                                filterVo.setAttrId(attrId);
                                filterVo.setExpression(SearchExpression.EQ.getExpression());
                                filterVo.setValueList(attrEntityVo.getValueList().stream().map(Object::toString).collect(Collectors.toList()));
                                ciEntityConditionVo.addAttrFilter(filterVo);
                            }
                        }
                    }
                    if (CollectionUtils.isNotEmpty(ciEntityConditionVo.getAttrFilterList())) {
                        List<CiEntityVo> checkList = this.searchCiEntity(ciEntityConditionVo);
                        for (CiEntityVo checkCiEntity : checkList) {
                            if (!checkCiEntity.getId().equals(ciEntityTransactionVo.getCiEntityId())) {
                                throw new CiUniqueRuleException(ciVo);
                            }
                        }
                    }
                }
            }
        }
        return true;
    }


    /**
     * 提交事务 修改规则:
     * editMode针对单次修改。editMode=Global下，提供属性或关系代表修改，不提供代表删除；editMode=Partial下，提供属性或关系代表修改，不提供代表不修改
     * saveMode针对单个属性或关系，saveMode=replace，代表覆盖，最后结果是修改或删除；saveMode=merge，代表合并，最后结果是添加新成员或维持不变。
     *
     * @param transactionVo      事务
     * @param transactionGroupVo 事务分组
     * @return Long 配置项id
     */
    private Long commitTransaction(TransactionVo transactionVo, TransactionGroupVo transactionGroupVo) {
        CiEntityTransactionVo ciEntityTransactionVo = transactionVo.getCiEntityTransactionVo();
        if (ciEntityTransactionVo.getAction().equals(TransactionActionType.DELETE.getValue())) { // 删除配置项
            //补充对端配置项事务信息
            List<RelEntityVo> relEntityList = relEntityMapper.getRelEntityByCiEntityId(ciEntityTransactionVo.getCiEntityId());
            //记录对端配置项的事务，如果同一个配置项则不需要添加多个事务
            Set<Long> ciEntityTransactionSet = new HashSet<>();
            if (CollectionUtils.isNotEmpty(relEntityList)) {
                for (RelEntityVo item : relEntityList) {
                    //如果不是自己引用自己，则需要补充对端配置项事务，此块需要在真正删除数据前处理
                    if (!item.getFromCiEntityId().equals(item.getToCiEntityId())) {
                        Long ciEntityId = null;
                        Long ciId = null;
                        //补充关系删除事务数据
                        RelVo relVo = relMapper.getRelById(item.getRelId());
                        boolean isCascadeDelete = false;
                        if (item.getDirection().equals(RelDirectionType.FROM.getValue())) {
                            ciEntityId = item.getToCiEntityId();
                            ciId = item.getToCiId();
                            if (relVo.getFromIsCascadeDelete().equals(1)) {
                                isCascadeDelete = true;
                            }
                        } else if (item.getDirection().equals(RelDirectionType.TO.getValue())) {
                            ciEntityId = item.getFromCiEntityId();
                            ciId = item.getFromCiId();
                            if (relVo.getToIsCascadeDelete().equals(1)) {
                                isCascadeDelete = true;
                            }
                        }

                        if (!ciEntityTransactionSet.contains(ciEntityId) && !transactionGroupVo.isExclude(ciEntityId)) {
                            if (isCascadeDelete) {
                                //级联删除
                                //将当前配置项加入忽略列表，这样做级联删除时，关联的配置项就不会通过反查找回当前配置项，从而产生一个不必要的修改事务
                                transactionGroupVo.addExclude(ciEntityTransactionVo.getCiEntityId());
                                Long cascadeTransactionId = deleteCiEntity(new CiEntityVo(ciEntityId), true, transactionGroupVo);
                                //由于有级联删除的需要，所以需要在这里建立事务和事务组的关系
                                if (cascadeTransactionId > 0L) {
                                    transactionMapper.insertTransactionGroup(transactionGroupVo.getId(), cascadeTransactionId);
                                }
                            } else {
                                //补充关系对端事务
                                CiEntityVo oldCiEntityVo = this.getCiEntityByIdLite(ciId, ciEntityId, true, false, false);
                                if (oldCiEntityVo != null) {
                                    TransactionVo toTransactionVo = new TransactionVo();
                                    toTransactionVo.setCiId(ciId);
                                    toTransactionVo.setInputFrom(transactionVo.getInputFrom());
                                    toTransactionVo.setStatus(TransactionStatus.COMMITED.getValue());
                                    toTransactionVo.setCreateUser(transactionVo.getCreateUser());
                                    toTransactionVo.setCommitUser(transactionVo.getCommitUser());
                                    toTransactionVo.setDescription(transactionVo.getDescription());
                                    CiEntityTransactionVo endCiEntityTransactionVo = new CiEntityTransactionVo();

                                    endCiEntityTransactionVo.setCiEntityId(ciEntityId);
                                    endCiEntityTransactionVo.setCiId(ciId);
                                    endCiEntityTransactionVo.setAction(TransactionActionType.UPDATE.getValue());
                                    endCiEntityTransactionVo.setTransactionId(toTransactionVo.getId());
                                    endCiEntityTransactionVo.setName(oldCiEntityVo.getName());
                                    endCiEntityTransactionVo.setOldCiEntityVo(oldCiEntityVo);
                                    createSnapshot(endCiEntityTransactionVo);

                                    //由于是补充对端关系，所以关系要取反
                                    endCiEntityTransactionVo.addRelEntityData(relVo, item.getDirection().equals(RelDirectionType.FROM.getValue()) ? RelDirectionType.TO.getValue() : RelDirectionType.FROM.getValue(), item.getDirection().equals(RelDirectionType.FROM.getValue()) ? item.getFromCiId() : item.getToCiId(), item.getDirection().equals(RelDirectionType.FROM.getValue()) ? item.getFromCiEntityId() : item.getToCiEntityId(), item.getDirection().equals(RelDirectionType.FROM.getValue()) ? item.getFromCiEntityName() : item.getToCiEntityName(), TransactionActionType.DELETE.getValue());

                                    transactionMapper.insertTransaction(toTransactionVo);
                                    transactionMapper.insertCiEntityTransaction(endCiEntityTransactionVo);
                                    transactionMapper.insertTransactionGroup(transactionGroupVo.getId(), toTransactionVo.getId());
                                    //发送消息到消息队列
                                    ITopic<CiEntityTransactionVo> topic = TopicFactory.getTopic("cmdb/cientity/update");
                                    if (topic != null) {
                                        topic.send(endCiEntityTransactionVo);
                                    }
                                }
                                //正式删除关系数据
                                relEntityMapper.deleteRelEntityByRelIdFromCiEntityIdToCiEntityId(item.getRelId(), item.getFromCiEntityId(), item.getToCiEntityId());
                                //删除级联关系数据
                                RelativeRelManager.delete(item);
                            }
                            ciEntityTransactionSet.add(ciEntityId);
                        }
                    }
                }
                //重建关系序列
                rebuildRelEntityIndex(relEntityList);
            }
            CiEntityVo deleteCiEntityVo = new CiEntityVo(ciEntityTransactionVo);
            //删除之前找到所有关联配置项，可能需要更新他们的表达式属性
            this.updateInvokedExpressionAttr(deleteCiEntityVo);

            this.deleteCiEntity(deleteCiEntityVo);

            //修改事务状态
            transactionVo.setCommitUser(UserContext.get().getUserId(true));
            transactionVo.setStatus(TransactionStatus.COMMITED.getValue());
            transactionMapper.updateTransactionStatus(transactionVo);

            //删除全文检索索引
            IFullTextIndexHandler handler = FullTextIndexHandlerFactory.getHandler(CmdbFullTextIndexType.CIENTITY);
            if (handler != null) {
                handler.deleteIndex(deleteCiEntityVo.getId());
            }

            //发送消息到消息队列
            ITopic<CiEntityTransactionVo> topic = TopicFactory.getTopic("cmdb/cientity/delete");
            if (topic != null) {
                topic.send(ciEntityTransactionVo);
            }
            return null;
        } else {
            /*
            写入属性信息
            1、如果是引用类型，并且是replace模式，需要先清空原来的值再写入。
             */
            CiVo ciVo = ciMapper.getCiById(ciEntityTransactionVo.getCiId());
            CiEntityVo ciEntityVo = new CiEntityVo(ciEntityTransactionVo);
            ciEntityVo.setExpiredDay(ciVo.getExpiredDay());
            List<AttrEntityVo> rebuildAttrEntityList = new ArrayList<>();
            for (AttrEntityTransactionVo attrEntityTransactionVo : ciEntityTransactionVo.getAttrEntityTransactionList()) {
                AttrEntityVo attrEntityVo = new AttrEntityVo(attrEntityTransactionVo);
                if (attrEntityVo.isNeedTargetCi()) {
                    if (attrEntityTransactionVo.getSaveMode().equals(SaveModeType.REPLACE.getValue())) {
                        ciEntityMapper.deleteAttrEntityByFromCiEntityIdAndAttrId(ciEntityTransactionVo.getCiEntityId(), attrEntityTransactionVo.getAttrId());
                    }
                    if (CollectionUtils.isNotEmpty(attrEntityVo.getValueList())) {
                        ciEntityMapper.insertAttrEntity(attrEntityVo);
                    }
                    rebuildAttrEntityList.add(attrEntityVo);
                    //更新配置项名称
                    if (Objects.equals(ciVo.getNameAttrId(), attrEntityVo.getAttrId())) {
                        List<CiEntityVo> invokeCiEntityList = ciEntityMapper.getCiEntityBaseInfoByAttrIdAndFromCiEntityId(ciEntityVo.getId(), attrEntityVo.getAttrId());
                        if (CollectionUtils.isNotEmpty(invokeCiEntityList)) {
                            ciEntityVo.setName(invokeCiEntityList.stream().map(CiEntityVo::getName).collect(Collectors.joining(",")));
                        } else {
                            ciEntityVo.setName("");
                        }
                        updateCiEntityName(ciEntityVo);
                    }
                } else {
                    //更新配置项名称
                    if (Objects.equals(ciVo.getNameAttrId(), attrEntityVo.getAttrId())) {
                        ciEntityVo.setName(attrEntityVo.getValue());
                        updateCiEntityName(ciEntityVo);
                    }
                }
            }
            //处理全局属性
            for (GlobalAttrEntityTransactionVo globalAttrTransactionVo : ciEntityTransactionVo.getGlobalAttrTransactionList()) {
                GlobalAttrEntityVo attrEntityVo = new GlobalAttrEntityVo(globalAttrTransactionVo);
                if (globalAttrTransactionVo.getSaveMode().equals(SaveModeType.REPLACE.getValue())) {
                    globalAttrMapper.deleteGlobalAttrEntityByCiEntityIdAndAttrId(ciEntityTransactionVo.getCiEntityId(), globalAttrTransactionVo.getAttrId());
                }
                if (CollectionUtils.isNotEmpty(attrEntityVo.getValueList())) {
                    globalAttrMapper.insertGlobalAttrEntityItem(attrEntityVo);
                }
            }

            //重建属性序列
            rebuildAttrEntityIndex(rebuildAttrEntityList);

            String topicName = "";
            /*
            写入配置项信息
             */
            if (ciEntityTransactionVo.getAction().equals(TransactionActionType.INSERT.getValue())) {
                this.insertCiEntity(ciEntityVo);
                topicName = "cmdb/cientity/insert";
            } else if (ciEntityTransactionVo.getAction().equals(TransactionActionType.UPDATE.getValue())) {
                this.updateCiEntity(ciEntityVo);
                topicName = "cmdb/cientity/update";
            } else if (ciEntityTransactionVo.getAction().equals(TransactionActionType.RECOVER.getValue())) {
                if (ciEntityMapper.getCiEntityBaseInfoById(ciEntityTransactionVo.getCiEntityId()) == null) {
                    this.insertCiEntity(ciEntityVo);
                } else {
                    //throw new CiEntityIsRecoveredException(ciEntityVo.getName());
                    this.updateCiEntity(ciEntityVo);
                }
                topicName = "cmdb/cientity/recover";
            }
            /*
            写入关系信息
             */
            List<RelEntityTransactionVo> relEntityTransactionList = ciEntityTransactionVo.getRelEntityTransactionList();
            if (CollectionUtils.isNotEmpty(relEntityTransactionList)) {
                //记录对端配置项的事务，如果同一个配置项则不需要添加多个事务
                Map<Long, CiEntityTransactionVo> ciEntityTransactionMap = new HashMap<>();
                List<RelEntityVo> rebuildRelEntityList = new ArrayList<>();
                for (RelEntityTransactionVo relEntityTransactionVo : relEntityTransactionList) {
                    //如果不是自己引用自己，则需要补充对端配置项事务，此块需要在真正删除数据前处理
                    if (!relEntityTransactionVo.getFromCiEntityId().equals(relEntityTransactionVo.getToCiEntityId())) {
                        Long ciEntityId = null;
                        Long ciId = null;
                        Long sourceCiEntityId = ciEntityTransactionVo.getCiEntityId();
                        Long sourceCiId = ciEntityTransactionVo.getCiId();
                        String sourceCiEntityName = ciEntityTransactionVo.getName();
                        if (relEntityTransactionVo.getDirection().equals(RelDirectionType.FROM.getValue())) {
                            ciEntityId = relEntityTransactionVo.getToCiEntityId();
                            ciId = relEntityTransactionVo.getToCiId();
                        } else if (relEntityTransactionVo.getDirection().equals(RelDirectionType.TO.getValue())) {
                            ciEntityId = relEntityTransactionVo.getFromCiEntityId();
                            ciId = relEntityTransactionVo.getFromCiId();
                        }

                        //排除掉当前事务的cientityId，不然有可能会重复插入事务
                        if (!transactionGroupVo.isExclude(ciEntityId) && !ciEntityTransactionMap.containsKey(ciEntityId)) {
                            TransactionVo toTransactionVo = new TransactionVo();
                            toTransactionVo.setCiId(ciId);
                            toTransactionVo.setInputFrom(transactionVo.getInputFrom());
                            toTransactionVo.setStatus(TransactionStatus.COMMITED.getValue());
                            toTransactionVo.setCreateUser(transactionVo.getCreateUser());
                            toTransactionVo.setCommitUser(transactionVo.getCommitUser());
                            toTransactionVo.setDescription(transactionVo.getDescription());
                            transactionMapper.insertTransaction(toTransactionVo);
                            transactionMapper.insertTransactionGroup(transactionGroupVo.getId(), toTransactionVo.getId());
                            CiEntityTransactionVo endCiEntityTransactionVo = new CiEntityTransactionVo();
                            endCiEntityTransactionVo.setCiEntityId(ciEntityId);
                            endCiEntityTransactionVo.setCiId(ciId);
                            endCiEntityTransactionVo.setAction(TransactionActionType.UPDATE.getValue());
                            endCiEntityTransactionVo.setTransactionId(toTransactionVo.getId());
                            endCiEntityTransactionVo.setOldCiEntityVo(this.getCiEntityByIdLite(ciId, ciEntityId, true, false, false));
                            createSnapshot(endCiEntityTransactionVo);
                            ciEntityTransactionMap.put(endCiEntityTransactionVo.getCiEntityId(), endCiEntityTransactionVo);
                        }

                        if (ciEntityTransactionMap.containsKey(ciEntityId)) {
                            CiEntityTransactionVo endCiEntityTransactionVo = ciEntityTransactionMap.get(ciEntityId);
                            //补充关系修改信息
                            RelVo relVo = relMapper.getRelById(relEntityTransactionVo.getRelId());
                            endCiEntityTransactionVo.addRelEntityData(relVo, relEntityTransactionVo.getDirection().equals(RelDirectionType.FROM.getValue()) ? RelDirectionType.TO.getValue() : RelDirectionType.FROM.getValue(), sourceCiId, sourceCiEntityId, sourceCiEntityName, relEntityTransactionVo.getAction());
                        }
                    }

                    if (relEntityTransactionVo.getAction().equals(RelActionType.DELETE.getValue())) {
                        RelEntityVo relEntityVo = relEntityMapper.getRelEntityByFromCiEntityIdAndToCiEntityIdAndRelId(relEntityTransactionVo.getFromCiEntityId(), relEntityTransactionVo.getToCiEntityId(), relEntityTransactionVo.getRelId());
                        relEntityMapper.deleteRelEntityByRelIdFromCiEntityIdToCiEntityId(relEntityTransactionVo.getRelId(), relEntityTransactionVo.getFromCiEntityId(), relEntityTransactionVo.getToCiEntityId());
                        rebuildRelEntityList.add(new RelEntityVo(relEntityTransactionVo));
                        //删除级联关系
                        RelativeRelManager.delete(relEntityVo);
                    } else if (relEntityTransactionVo.getAction().equals(RelActionType.INSERT.getValue())
                            || relEntityTransactionVo.getAction().equals(RelActionType.REPLACE.getValue())
                            || relEntityTransactionVo.getAction().equals(RelActionType.UPDATE.getValue())) {
                        RelEntityVo newRelEntityVo = new RelEntityVo(relEntityTransactionVo);
                        if (relEntityMapper.checkRelEntityIsExists(newRelEntityVo) == 0) {
                            relEntityMapper.insertRelEntity(newRelEntityVo);
                            rebuildRelEntityList.add(newRelEntityVo);
                        }
                        //添加级联关系
                        RelativeRelManager.insert(newRelEntityVo);
                    }
                }
                //所有事务信息补充完毕后才能写入，因为对端配置项有可能被引用多次
                for (Map.Entry<Long, CiEntityTransactionVo> entry : ciEntityTransactionMap.entrySet()) {
                    Long ciEntityId = entry.getKey();
                    transactionMapper.insertCiEntityTransaction(entry.getValue());
                    //发送消息到消息队列
                    ITopic<CiEntityTransactionVo> topic = TopicFactory.getTopic("cmdb/cientity/update");
                    if (topic != null) {
                        topic.send(ciEntityTransactionMap.get(ciEntityId));
                    }
                }
                //重建关系序列
                rebuildRelEntityIndex(rebuildRelEntityList);
            }

            //重新计算所有表达式属性的值
            AttrExpressionRebuildManager.rebuild(new RebuildAuditVo(ciEntityVo, RebuildAuditVo.Type.INVOKE));

            //重新计算引用了当前配置项的所有表达式属性的值
            if (ciEntityTransactionVo.getAction().equals(TransactionActionType.UPDATE.getValue())) {
                AttrExpressionRebuildManager.rebuild(new RebuildAuditVo(ciEntityVo, RebuildAuditVo.Type.INVOKED));
            }

            // 解除配置项修改锁定
            ciEntityVo.setIsLocked(0);
            ciEntityMapper.updateCiEntityLockById(ciEntityVo);

            //修改事务状态
            if (ciEntityTransactionVo.getAction().equals(TransactionActionType.RECOVER.getValue())) {
                transactionVo.setStatus(TransactionStatus.RECOVER.getValue());
                transactionVo.setRecoverUser(UserContext.get().getUserUuid(true));
            } else {
                transactionVo.setStatus(TransactionStatus.COMMITED.getValue());
                transactionVo.setCommitUser(UserContext.get().getUserUuid(true));
            }
            transactionMapper.updateTransactionStatus(transactionVo);

            //触发圈子归属判定
            CiEntityGroupManager.groupCiEntity(ciEntityVo.getCiId(), ciEntityVo.getId());

            //创建全文检索索引
            IFullTextIndexHandler handler = FullTextIndexHandlerFactory.getHandler(CmdbFullTextIndexType.CIENTITY);
            if (handler != null) {
                handler.createIndex(ciEntityVo.getId());
            }

            //发送消息到消息队列
            if (StringUtils.isNotBlank(topicName)) {
                ITopic<CiEntityTransactionVo> topic = TopicFactory.getTopic(topicName);
                if (topic != null) {
                    topic.send(ciEntityTransactionVo);
                }
            }
            return ciEntityVo.getId();
        }
    }

    /**
     * 重建属性序列号优化搜索性能，至少创建50条数据索引
     *
     * @param attrId         关系id
     * @param fromCiEntityId 配置项id
     */
    @Override
    public void rebuildAttrEntityIndex(Long attrId, Long fromCiEntityId) {
        attrEntityMapper.clearAttrEntityFromIndex(attrId, fromCiEntityId);
        List<AttrEntityVo> attrEntityList = attrEntityMapper.getAttrEntityByFromCiEntityIdAndAttrId(fromCiEntityId, attrId, Math.max(CiEntityVo.MAX_RELENTITY_COUNT + 1, 50));
        if (CollectionUtils.isNotEmpty(attrEntityList)) {
            for (int i = 0; i < attrEntityList.size(); i++) {
                AttrEntityVo attr = attrEntityList.get(i);
                attr.setFromIndex(i + 1);
                attrEntityMapper.updateAttrEntityFromIndex(attr);
            }
        }
    }

    /**
     * 重建序列号优化搜索，至少创建50条数据索引，留下足够扩展控件。
     */
    @Override
    public void rebuildRelEntityIndex(RelDirectionType direction, Long relId, Long ciEntityId) {
        List<RelEntityVo> relEntityList;
        if (direction == RelDirectionType.FROM) {
            relEntityMapper.clearRelEntityFromIndex(relId, ciEntityId, 50);
            relEntityList = relEntityMapper.getRelEntityByFromCiEntityIdAndRelId(ciEntityId, relId, Math.max(CiEntityVo.MAX_RELENTITY_COUNT + 1, 50));
            if (CollectionUtils.isNotEmpty(relEntityList)) {
                for (int i = 0; i < relEntityList.size(); i++) {
                    RelEntityVo rel = relEntityList.get(i);
                    rel.setFromIndex(i + 1);
                    relEntityMapper.updateRelEntityFromIndex(rel);
                }
            }
        } else if (direction == RelDirectionType.TO) {
            relEntityMapper.clearRelEntityToIndex(relId, ciEntityId, 50);
            relEntityList = relEntityMapper.getRelEntityByToCiEntityIdAndRelId(ciEntityId, relId, Math.max(CiEntityVo.MAX_RELENTITY_COUNT + 1, 50));
            if (CollectionUtils.isNotEmpty(relEntityList)) {
                for (int i = 0; i < relEntityList.size(); i++) {
                    RelEntityVo rel = relEntityList.get(i);
                    rel.setToIndex(i + 1);
                    relEntityMapper.updateRelEntityToIndex(rel);
                }
            }
        }
    }

    private void rebuildAttrEntityIndex(List<AttrEntityVo> pAttrEntityList) {
        //在事务处理完毕后再处理所有索引重建，避免属性对象也是新添加配置项的场景中，由于数据写入的顺序问题导致找不到数据而无法重建索引的问题
        AfterTransactionJob<List<AttrEntityVo>> job = new AfterTransactionJob<>("REBUILD-ATTRENTITY-INDEX");
        job.execute(pAttrEntityList, attrEntityList -> {
            if (CollectionUtils.isNotEmpty(attrEntityList)) {
                for (AttrEntityVo attrEntityVo : attrEntityList) {
                    rebuildAttrEntityIndex(attrEntityVo.getAttrId(), attrEntityVo.getFromCiEntityId());
                }
            }
        });
    }

    private void rebuildRelEntityIndex(List<RelEntityVo> pRelEntityList) {
        //在事务处理完毕后再处理所有关系索引重建，避免关系对象也是新添加的场景由于数据写入的顺序问题导致找不到数据而无法重建索引
        AfterTransactionJob<List<RelEntityVo>> job = new AfterTransactionJob<>("REBUILD-RELENTITY-INDEX");
        job.execute(pRelEntityList, relEntityList -> {
            if (CollectionUtils.isNotEmpty(relEntityList)) {
                Set<String> fromSet = relEntityList.stream().map(rel -> rel.getRelId() + "_" + rel.getFromCiEntityId()).collect(Collectors.toSet());
                Set<String> toSet = relEntityList.stream().map(rel -> rel.getRelId() + "_" + rel.getToCiEntityId()).collect(Collectors.toSet());
                if (CollectionUtils.isNotEmpty(fromSet)) {
                    for (String relId : fromSet) {
                        rebuildRelEntityIndex(RelDirectionType.FROM, Long.parseLong(relId.split("_")[0]), Long.parseLong(relId.split("_")[1]));
                    }
                }
                if (CollectionUtils.isNotEmpty(toSet)) {
                    for (String relId : toSet) {
                        rebuildRelEntityIndex(RelDirectionType.TO, Long.parseLong(relId.split("_")[0]), Long.parseLong(relId.split("_")[1]));
                    }
                }
            }
        });
    }

    /**
     * 更新所有关联了当前配置配置项的表达式属性，一般用在删除
     * 1、先判断当前配置项模型是否有被表达式属性引用。
     * 2、如果有则查出所有关联配置项放入重建记录里。
     *
     * @param ciEntityVo 配置项信息
     */
    private void updateInvokedExpressionAttr(CiEntityVo ciEntityVo) {
        List<Long> ciIdList = attrMapper.getExpressionCiIdByValueCiId(ciEntityVo.getCiId());
        if (CollectionUtils.isNotEmpty(ciIdList)) {
            List<RelEntityVo> relEntityList = relEntityMapper.getRelEntityByCiEntityId(ciEntityVo.getId());
            //排除掉没有表达式属性的模型
            relEntityList.removeIf(rel -> !ciIdList.contains(rel.getFromCiId()) && !ciIdList.contains(rel.getToCiId()));
            //排除掉自我引用
            relEntityList.removeIf(rel -> rel.getFromCiEntityId().equals(ciEntityVo.getId()) && rel.getToCiEntityId().equals(ciEntityVo.getId()));
            if (CollectionUtils.isNotEmpty(relEntityList)) {
                List<RebuildAuditVo> auditList = new ArrayList<>();
                for (RelEntityVo relEntityVo : relEntityList) {
                    RebuildAuditVo rebuildAuditVo = new RebuildAuditVo();
                    rebuildAuditVo.setCiId(relEntityVo.getDirection().equals(RelDirectionType.FROM.getValue()) ? relEntityVo.getToCiId() : relEntityVo.getFromCiId());
                    rebuildAuditVo.setCiEntityId(relEntityVo.getDirection().equals(RelDirectionType.FROM.getValue()) ? relEntityVo.getToCiEntityId() : relEntityVo.getFromCiEntityId());
                    rebuildAuditVo.setType(RebuildAuditVo.Type.INVOKE.getValue());
                    auditList.add(rebuildAuditVo);
                }
                AttrExpressionRebuildManager.rebuild(auditList);
            }
        }
    }

    /**
     * 写入配置项
     *
     * @param ciEntityVo 配置项信息
     */
    private void insertCiEntity(CiEntityVo ciEntityVo) {
        //记录原来的ciId，后面需要还原
        Long ciId = ciEntityVo.getCiId();
        CiVo ciVo = ciMapper.getCiById(ciEntityVo.getCiId());
        List<CiVo> ciList = ciMapper.getUpwardCiListByLR(ciVo.getLft(), ciVo.getRht());
        ciEntityMapper.insertCiEntityBaseInfo(ciEntityVo);
        if (ciEntityVo.getExpiredDay() != null && ciEntityVo.getExpiredDay() > 0) {
            ciEntityMapper.insertCiEntityExpiredTime(ciEntityVo);
        } else {
            ciEntityMapper.deleteCiEntityExpiredTimeByCiEntityId(ciEntityVo.getId());
        }

        for (CiVo ci : ciList) {
            ciEntityVo.setCiId(ci.getId());
            ciEntityMapper.insertCiEntity(ciEntityVo);
        }
        ciEntityVo.setCiId(ciId);
    }

    /**
     * 更新配置项
     *
     * @param ciEntityVo 配置项信息
     */
    @Override
    public void updateCiEntity(CiEntityVo ciEntityVo) {
        //记录原来的ciId，后面需要还原
        Long ciId = ciEntityVo.getCiId();
        CiVo ciVo = ciMapper.getCiById(ciEntityVo.getCiId());
        List<CiVo> ciList = ciMapper.getUpwardCiListByLR(ciVo.getLft(), ciVo.getRht());
        ciEntityMapper.updateCiEntityBaseInfo(ciEntityVo);
        if (ciEntityVo.getExpiredDay() != null && ciEntityVo.getExpiredDay() > 0) {
            ciEntityMapper.insertCiEntityExpiredTime(ciEntityVo);
        } else {
            ciEntityMapper.deleteCiEntityExpiredTimeByCiEntityId(ciEntityVo.getId());
        }
        for (CiVo ci : ciList) {
            if (ciEntityVo.getAttrEntityList().stream().anyMatch(attr -> !attr.isNeedTargetCi() && attr.getFromCiId().equals(ci.getId()))) {
                ciEntityVo.setCiId(ci.getId());
                ciEntityMapper.updateCiEntity(ciEntityVo);
            }
        }
        ciEntityVo.setCiId(ciId);
    }

    /**
     * 删除配置项
     *
     * @param ciEntityVo 配置项信息
     */
    private void deleteCiEntity(CiEntityVo ciEntityVo) {
        CiVo ciVo = ciMapper.getCiById(ciEntityVo.getCiId());
        List<CiVo> ciList = ciMapper.getUpwardCiListByLR(ciVo.getLft(), ciVo.getRht());
        ciEntityMapper.deleteCiEntityBaseInfo(ciEntityVo);
        //删除全局属性
        globalAttrMapper.deleteGlobalAttrEntityByCiEntityId(ciEntityVo.getId());
        for (CiVo ci : ciList) {
            ciEntityVo.setCiId(ci.getId());
            ciEntityMapper.deleteCiEntity(ciEntityVo);
        }
    }

    /**
     * 提交事务，返回配置项id
     */
    @Override
    @Transactional
    public List<TransactionStatusVo> commitTransactionGroup(TransactionGroupVo transactionGroupVo) {
        List<TransactionStatusVo> statusList = new ArrayList<>();
        for (TransactionVo transactionVo : transactionGroupVo.getTransactionList()) {
            transactionGroupVo.addExclude(transactionVo.getCiEntityTransactionVo().getCiEntityId());
        }
        for (TransactionVo transactionVo : transactionGroupVo.getTransactionList()) {
            if (transactionVo.getStatus().equals(TransactionStatus.COMMITED.getValue())) {
                throw new TransactionStatusIrregularException(TransactionStatus.COMMITED);
            } else if (transactionVo.getStatus().equals(TransactionStatus.RECOVER.getValue())) {
                throw new TransactionStatusIrregularException(TransactionStatus.RECOVER);
            }
            if (!CiAuthChecker.chain().checkCiEntityTransactionPrivilege(transactionVo.getCiId()).checkCiEntityIsInGroup(transactionVo.getCiEntityId(), GroupType.MAINTAIN).check()) {
                throw new TransactionAuthException();
            }
            try {
                if (validateCiEntityTransactionForCommit(transactionVo.getCiEntityTransactionVo())) {
                    this.commitTransaction(transactionVo, transactionGroupVo);
                    statusList.add(new TransactionStatusVo(transactionVo.getId(), TransactionStatus.COMMITED));
                }
            } catch (Exception ex) {
                AfterTransactionJob<TransactionVo> job = new AfterTransactionJob<>("CIENTITY-UPDATE-TRANSACTION-STATUS");
                job.execute(transactionVo, t -> {
                }, t -> {
                    t.setError(ex.getMessage());
                    t.setStatus(TransactionStatus.UNCOMMIT.getValue());
                    transactionMapper.updateTransactionStatus(t);
                });
                throw ex;
            }
        }
        return statusList;
    }

    @Transactional
    @Override
    public void recoverCiEntity(TransactionVo transactionVo) {
        transactionVo.getCiEntityTransactionVo().restoreSnapshot();
        CiEntityTransactionVo ciEntityTransactionVo = transactionVo.getCiEntityTransactionVo();
        transactionVo.setAction(TransactionActionType.RECOVER.getValue());
        transactionVo.getCiEntityTransactionVo().setAction(TransactionActionType.RECOVER.getValue());
        if (validateCiEntityTransaction(ciEntityTransactionVo)) {
            this.commitTransaction(transactionVo, new TransactionGroupVo());
        }
        transactionMapper.updateTransactionStatus(transactionVo);
    }

    @Transactional
    @Override
    public void recoverTransactionGroup(Long transactionGroupId) {
        List<TransactionVo> transactionList = transactionMapper.getTransactionByGroupId(transactionGroupId);
        for (TransactionVo transactionVo : transactionList) {
            recoverCiEntity(transactionVo);
        }
    }

    @Override
    public void getCiViewMapAndAttrMapAndRelMap(Long ciId, Map<Long, AttrVo> attrMap, Map<Long, RelVo> relMap, Map<String, CiViewVo> ciViewMap, Map<Long, GlobalAttrVo> globalAttrMap) {
        List<AttrVo> attrList = attrMapper.getAttrByCiId(ciId);
        attrMap.putAll(attrList.stream().collect(Collectors.toMap(AttrVo::getId, e -> e)));
        List<RelVo> relList = relMapper.getRelByCiId(ciId);
        for (RelVo relVo : relList) {
            relMap.put(relVo.getId(), relVo);
        }
        GlobalAttrVo searchVo = new GlobalAttrVo();
        searchVo.setIsActive(1);
        List<GlobalAttrVo> globalAttrList = globalAttrMapper.searchGlobalAttr(searchVo);
        globalAttrMap.putAll(globalAttrList.stream().collect(Collectors.toMap(GlobalAttrVo::getId, e -> e)));

        CiViewVo ciViewVo = new CiViewVo();
        ciViewVo.setCiId(ciId);
        List<CiViewVo> ciViewList = RelUtil.ClearCiViewRepeatRel(ciViewMapper.getCiViewByCiId(ciViewVo));
        for (CiViewVo ciview : ciViewList) {
            switch (ciview.getType()) {
                case "attr":
                    ciViewMap.put("attr_" + ciview.getItemId(), ciview);
                    break;
                case "relfrom":
                    ciViewMap.put("relfrom_" + ciview.getItemId(), ciview);
                    break;
                case "relto":
                    ciViewMap.put("relto_" + ciview.getItemId(), ciview);
                    break;
                case "const":
                    //固化属性需要特殊处理
                    ciViewMap.put("const_" + ciview.getItemName().replace("_", ""), ciview);
                    break;
                case "global":
                    ciViewMap.put("global_" + ciview.getItemId(), ciview);
                    break;
            }
        }
    }

    @Override
    public AttrFilterVo convertAttrFilter(AttrVo attrVo, String expression, List<String> valueList) {
        AttrFilterVo attrFilterVo = new AttrFilterVo();
        attrFilterVo.setAttrId(attrVo.getId());
        if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NULL.getExpression())
                || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NOTNULL.getExpression())) {
            attrFilterVo.setExpression(expression);
            return attrFilterVo;
        }
        if (StringUtils.isBlank(expression)) {
            expression = neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression();
        }
        if ("select".equals(attrVo.getType())) {
            CiVo targetCiVo = ciMapper.getCiById(attrVo.getTargetCiId());
            if (targetCiVo == null) {
                return null;
            }
            List<CiVo> downwardCiList = ciMapper.getDownwardCiListByLR(targetCiVo.getLft(), targetCiVo.getRht());
            Map<Long, CiVo> downwardCiMap = downwardCiList.stream().collect(Collectors.toMap(e -> e.getId(), e -> e));

            CiEntityVo ciEntityVo = new CiEntityVo();
            ciEntityVo.setCiId(targetCiVo.getId());
            ciEntityVo.setIdList(new ArrayList<>(downwardCiMap.keySet()));
            List<String> newValueList = new ArrayList<>();
            for (String value : valueList) {
                List<CiEntityVo> ciEntityList = new ArrayList<>();
                ciEntityVo.setName(value);
                if (Objects.equals(targetCiVo.getIsVirtual(), 1)) {
                    if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.LI.getExpression())
                            || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NL.getExpression())) {
                        ciEntityList = ciEntityMapper.getVirtualCiEntityBaseInfoByLikeName(ciEntityVo);
                    } else if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression())
                            || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NE.getExpression())) {
                        ciEntityList = ciEntityMapper.getVirtualCiEntityBaseInfoByName(ciEntityVo);
                    }
                } else {
                    if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.LI.getExpression())
                            || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NL.getExpression())) {
                        ciEntityList = ciEntityMapper.getCiEntityListByCiIdListAndLikeName(ciEntityVo);
                    } else if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression())
                            || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NE.getExpression())) {
                        ciEntityList = ciEntityMapper.getCiEntityListByCiIdListAndName(ciEntityVo);
                    }
                }
                for (CiEntityVo ciEntity : ciEntityList) {
                    newValueList.add(ciEntity.getId().toString());
                }
            }
            if (CollectionUtils.isEmpty(newValueList)) {
                if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.LI.getExpression())
                        || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression())) {
                    return null;
                }
            }
            attrFilterVo.setValueList(newValueList);
        } else {
            attrFilterVo.setValueList(valueList);
        }
        attrFilterVo.setExpression(expression);
        return attrFilterVo;
    }

    @Override
    public GlobalAttrFilterVo convertGlobalAttrFilter(GlobalAttrVo globalAttrVo, String expression, List<String> valueList) {
        GlobalAttrFilterVo globalAttrFilterVo = new GlobalAttrFilterVo();
        globalAttrFilterVo.setAttrId(globalAttrVo.getId());
        if (StringUtils.isBlank(expression)) {
            expression = neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression();
        }
        if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NULL.getExpression())
                || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NOTNULL.getExpression())) {
            globalAttrFilterVo.setExpression(expression);
            return globalAttrFilterVo;
        }
        List<Long> longValueList = new ArrayList<>();
        List<GlobalAttrItemVo> itemList = globalAttrMapper.getAllGlobalAttrItemByAttrId(globalAttrVo.getId());
        Map<String, GlobalAttrItemVo> globalAttrItemMap = itemList.stream().collect(Collectors.toMap(GlobalAttrItemVo::getValue, e -> e));
        for (String value : valueList) {
            GlobalAttrItemVo globalAttrItemVo = globalAttrItemMap.get(value);
            for (Map.Entry<String, GlobalAttrItemVo> entry : globalAttrItemMap.entrySet()) {
                String key = entry.getKey();
                if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression())
                        || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NE.getExpression())) {
                    if (Objects.equals(key, value)) {
                        globalAttrItemVo = entry.getValue();
                    }
                } else if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.LI.getExpression())
                        || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NL.getExpression())) {
                    if (key.toLowerCase().contains(value.toLowerCase())) {
                        globalAttrItemVo = entry.getValue();
                    }
                }
            }
            if (globalAttrItemVo == null) {
                return null;
            }
            longValueList.add(globalAttrItemVo.getId());
        }
        if (CollectionUtils.isEmpty(longValueList)) {
            return null;
        }
        globalAttrFilterVo.setValueList(longValueList);

        if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression())) {
            expression = neatlogic.framework.matrix.constvalue.SearchExpression.LI.getExpression();
        } else if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NE.getExpression())) {
            expression = neatlogic.framework.matrix.constvalue.SearchExpression.NL.getExpression();
        }
        globalAttrFilterVo.setExpression(expression);
        globalAttrFilterVo.setName(globalAttrVo.getName());
        globalAttrFilterVo.setLabel(globalAttrVo.getLabel());
        return globalAttrFilterVo;
    }

    @Override
    public RelFilterVo convertFromRelFilter(RelVo relVo, String expression, List<String> valueList, String direction) {
        if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NULL.getExpression())
                || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NOTNULL.getExpression())) {
            RelFilterVo relFilterVo = new RelFilterVo();
            relFilterVo.setRelId(relVo.getId());
            relFilterVo.setExpression(expression);
            relFilterVo.setDirection(direction);
            return relFilterVo;
        }
        Long ciId = null;
        if ("from".equals(direction)) {
            ciId = relVo.getToCiId();
        } else if ("to".equals(direction)) {
            ciId = relVo.getFromCiId();
        } else {
            return null;
        }
        CiVo ciVo = ciMapper.getCiById(ciId);
        if (ciVo == null) {
            return null;
        }
        if (StringUtils.isBlank(expression)) {
            expression = neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression();
        }
        List<Long> ciEntityIdList = new ArrayList<>();
        for (String value : valueList) {
            RelEntityVo relEntityVo = new RelEntityVo();
            relEntityVo.setRelId(relVo.getId());
            relEntityVo.setPageSize(100);
            if ("from".equals(direction)) {
                relEntityVo.setToCiEntityName(value);
                List<RelEntityVo> relEntityList = new ArrayList<>();
                if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.LI.getExpression())
                        || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NL.getExpression())) {
                    relEntityList = relEntityMapper.getRelEntityByRelIdAndLikeToCiEntityName(relEntityVo);
                } else if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression())
                        || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NE.getExpression())) {
                    relEntityList = relEntityMapper.getRelEntityByRelIdAndToCiEntityName(relEntityVo);
                }
                for (RelEntityVo relEntity : relEntityList) {
                    ciEntityIdList.add(relEntity.getToCiEntityId());
                }
            } else if ("to".equals(direction)) {
                relEntityVo.setFromCiEntityName(value);
                List<RelEntityVo> relEntityList = new ArrayList<>();
                if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.LI.getExpression())
                        || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NL.getExpression())) {
                    relEntityList = relEntityMapper.getRelEntityByRelIdAndLikeFromCiEntityName(relEntityVo);
                } else if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression())
                        || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NE.getExpression())) {
                    relEntityList = relEntityMapper.getRelEntityByRelIdAndFromCiEntityName(relEntityVo);
                }
                for (RelEntityVo relEntity : relEntityList) {
                    ciEntityIdList.add(relEntity.getFromCiEntityId());
                }
            }
        }
        if (CollectionUtils.isEmpty(ciEntityIdList)) {
            if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression()) || Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.LI.getExpression())) {
                return null;
            }
        }

        if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.EQ.getExpression())) {
            expression = neatlogic.framework.matrix.constvalue.SearchExpression.LI.getExpression();
        } else if (Objects.equals(expression, neatlogic.framework.matrix.constvalue.SearchExpression.NE.getExpression())) {
            expression = neatlogic.framework.matrix.constvalue.SearchExpression.NL.getExpression();
        }
        RelFilterVo relFilterVo = new RelFilterVo();
        relFilterVo.setRelId(relVo.getId());
        relFilterVo.setExpression(expression);
        relFilterVo.setValueList(ciEntityIdList);
        relFilterVo.setDirection(direction);
        return relFilterVo;
    }

    @Override
    public JSONObject getTbodyRowData(List<String> viewConstNameList, CiEntityVo ciEntity) {
        JSONObject tbody = new JSONObject();
        if (CollectionUtils.isNotEmpty(viewConstNameList)) {
            String ciEntityToJSONString = JSON.toJSONString(ciEntity);
            for (String viewConstName : viewConstNameList) {
                Object viewConstValue = JSONPath.read(ciEntityToJSONString, viewConstName.replace("_", ""));
                if (viewConstValue != null) {
                    tbody.put("const" + viewConstName, viewConstValue);
                } else {
                    tbody.put("const" + viewConstName, "");
                }
            }
        }
        tbody.putAll(getTbodyRowData(ciEntity));
        return tbody;
    }

    @Override
    public JSONObject getTbodyRowData(CiEntityVo ciEntity) {
        JSONObject tbody = new JSONObject();
        JSONObject attrEntityData = ciEntity.getAttrEntityData();
        if (MapUtils.isNotEmpty(attrEntityData)) {
            for (Map.Entry<String, Object> entry : attrEntityData.entrySet()) {
                JSONObject valueObj = (JSONObject) entry.getValue();
                String key = entry.getKey();
                if (StringUtils.isNotBlank(key)) {
                    JSONArray actualValueArray = valueObj.getJSONArray("actualValueList");
                    if (CollectionUtils.isNotEmpty(actualValueArray)) {
                        List<String> actualValueList = actualValueArray.toJavaList(String.class);
                        tbody.put(key, String.join(",", actualValueList));
                    }
                }
            }
        }
        JSONObject relEntityData = ciEntity.getRelEntityData();
        if (MapUtils.isNotEmpty(relEntityData)) {
            for (Map.Entry<String, Object> entry : relEntityData.entrySet()) {
                JSONObject relObj = (JSONObject) entry.getValue();
                String key = entry.getKey();
                if (StringUtils.isNotBlank(key)) {
                    JSONArray valueArray = relObj.getJSONArray("valueList");
                    if (CollectionUtils.isNotEmpty(valueArray)) {
                        List<String> ciEntityNameList = new ArrayList<>();
                        for (int j = 0; j < valueArray.size(); j++) {
                            JSONObject valueObj = valueArray.getJSONObject(j);
                            String ciEntityName = valueObj.getString("ciEntityName");
                            if (StringUtils.isNotBlank(ciEntityName)) {
                                ciEntityNameList.add(ciEntityName);
                            }
                        }
                        tbody.put(key, String.join(",", ciEntityNameList));
                    }
                }
            }
        }
        JSONObject globalAttrEntityData = ciEntity.getGlobalAttrEntityData();
        if (MapUtils.isNotEmpty(globalAttrEntityData)) {
            for (Map.Entry<String, Object> entry : globalAttrEntityData.entrySet()) {
                JSONObject valueObj = (JSONObject) entry.getValue();
                String key = entry.getKey();
                if (StringUtils.isNotBlank(key)) {
                    JSONArray valueArray = valueObj.getJSONArray("valueList");
                    if (CollectionUtils.isNotEmpty(valueArray)) {
                        List<GlobalAttrItemVo> valueList = valueArray.toJavaList(GlobalAttrItemVo.class);
                        if (CollectionUtils.isNotEmpty(valueList)) {
                            List<String> list = new ArrayList<>();
                            for (GlobalAttrItemVo globalAttrItemVo : valueList) {
                                list.add(globalAttrItemVo.getValue());
                            }
                            tbody.put(key, String.join(",", list));
                        }
                    }
                }
            }
        }
        return tbody;
    }

}
