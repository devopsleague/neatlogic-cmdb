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

package neatlogic.module.cmdb.process.stephandler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.cmdb.dto.transaction.TransactionVo;
import neatlogic.framework.crossover.CrossoverServiceFactory;
import neatlogic.framework.notify.crossover.INotifyServiceCrossoverService;
import neatlogic.framework.notify.dto.InvokeNotifyPolicyConfigVo;
import neatlogic.framework.process.operationauth.core.IOperationType;
import neatlogic.framework.process.constvalue.ProcessTaskOperationType;
import neatlogic.framework.process.constvalue.ProcessTaskStepOperationType;
import neatlogic.framework.process.crossover.IProcessTaskStepDataCrossoverMapper;
import neatlogic.framework.process.dto.ProcessStepVo;
import neatlogic.framework.process.dto.ProcessStepWorkerPolicyVo;
import neatlogic.framework.process.dto.ProcessTaskStepDataVo;
import neatlogic.framework.process.dto.ProcessTaskStepVo;
import neatlogic.framework.process.dto.processconfig.ActionConfigActionVo;
import neatlogic.framework.process.dto.processconfig.ActionConfigVo;
import neatlogic.framework.process.stephandler.core.ProcessStepInternalHandlerBase;
import neatlogic.framework.process.util.ProcessConfigUtil;
import neatlogic.framework.util.TableResultUtil;
import neatlogic.module.cmdb.process.dto.*;
import neatlogic.module.cmdb.process.exception.CiEntityConfigIllegalException;
import neatlogic.module.cmdb.process.notifyhandler.CmdbSyncNotifyHandler;
import neatlogic.module.cmdb.service.transaction.TransactionService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;
@Deprecated
//@Service
public class CmdbSyncProcessUtilHandler extends ProcessStepInternalHandlerBase {

    private final Logger logger = LoggerFactory.getLogger(CmdbSyncProcessUtilHandler.class);

    @Resource
    private TransactionService transactionService;

    @Override
    public String getHandler() {
        return CmdbProcessStepHandlerType.CMDBSYNC.getHandler();
    }

    @Override
    public Object getStartStepInfo(ProcessTaskStepVo currentProcessTaskStepVo) {
        return getNonStartStepInfo(currentProcessTaskStepVo);
    }

    @Override
    public Object getNonStartStepInfo(ProcessTaskStepVo currentProcessTaskStepVo) {
        IProcessTaskStepDataCrossoverMapper processTaskStepDataCrossoverMapper = CrossoverServiceFactory.getApi(IProcessTaskStepDataCrossoverMapper.class);
        JSONObject resultObj = new JSONObject();
        /* 事务审计列表 **/
        ProcessTaskStepDataVo search = new ProcessTaskStepDataVo();
        search.setProcessTaskId(currentProcessTaskStepVo.getProcessTaskId());
        search.setProcessTaskStepId(currentProcessTaskStepVo.getId());
        search.setType("ciEntitySyncResult");
        ProcessTaskStepDataVo processTaskStepData = processTaskStepDataCrossoverMapper.getProcessTaskStepData(search);
        if (processTaskStepData != null) {
            JSONObject dataObj = processTaskStepData.getData();
            JSONArray transactionGroupArray = dataObj.getJSONArray("transactionGroupList");
            if (CollectionUtils.isNotEmpty(transactionGroupArray)) {
                List<JSONObject> tableList = new ArrayList<>();
                for (int i = transactionGroupArray.size() - 1; i >= 0; i--) {
                    JSONObject transactionGroupObj = transactionGroupArray.getJSONObject(i);
                    Long time = transactionGroupObj.getLong("time");
                    Long transactionGroupId = transactionGroupObj.getLong("transactionGroupId");
                    TransactionVo transactionVo = new TransactionVo();
                    transactionVo.setTransactionGroupId(transactionGroupId);
                    List<TransactionVo> tbodyList = transactionService.searchTransaction(transactionVo);
                    if (CollectionUtils.isNotEmpty(tbodyList)) {
                        JSONObject tableObj = TableResultUtil.getResult(tbodyList);
                        tableObj.put("time", time);
                        tableList.add(tableObj);
                    }
                }
                resultObj.put("tableList", tableList);
            }
        }
        /* 错误信息列表 **/
        ProcessTaskStepDataVo searchVo = new ProcessTaskStepDataVo();
        searchVo.setProcessTaskId(currentProcessTaskStepVo.getProcessTaskId());
        searchVo.setProcessTaskStepId(currentProcessTaskStepVo.getId());
        searchVo.setType("ciEntitySyncError");
        ProcessTaskStepDataVo processTaskStepDataVo = processTaskStepDataCrossoverMapper.getProcessTaskStepData(searchVo);
        if (processTaskStepDataVo != null) {
            JSONObject dataObj = processTaskStepDataVo.getData();
            if (MapUtils.isNotEmpty(dataObj)) {
                resultObj.putAll(dataObj);
//                JSONArray errorList = dataObj.getJSONArray("errorList");
//                if (CollectionUtils.isNotEmpty(errorList)) {
//                    resultObj.put("errorList", errorList);
//                }
            }
        }
        return resultObj;
    }

    @Override
    public void makeupProcessStep(ProcessStepVo processStepVo, JSONObject stepConfigObj) {
        /* 组装通知策略id **/
        JSONObject notifyPolicyConfig = stepConfigObj.getJSONObject("notifyPolicyConfig");
        InvokeNotifyPolicyConfigVo invokeNotifyPolicyConfigVo = JSONObject.toJavaObject(notifyPolicyConfig, InvokeNotifyPolicyConfigVo.class);
        if (invokeNotifyPolicyConfigVo != null) {
            processStepVo.setNotifyPolicyConfig(invokeNotifyPolicyConfigVo);
        }

        JSONObject actionConfig = stepConfigObj.getJSONObject("actionConfig");
        ActionConfigVo actionConfigVo = JSONObject.toJavaObject(actionConfig, ActionConfigVo.class);
        if (actionConfigVo != null) {
            List<ActionConfigActionVo> actionList = actionConfigVo.getActionList();
            if (CollectionUtils.isNotEmpty(actionList)) {
                List<String> integrationUuidList = new ArrayList<>();
                for (ActionConfigActionVo actionVo : actionList) {
                    String integrationUuid = actionVo.getIntegrationUuid();
                    if (StringUtils.isNotBlank(integrationUuid)) {
                        integrationUuidList.add(integrationUuid);
                    }
                }
                processStepVo.setIntegrationUuidList(integrationUuidList);
            }
        }

        /* 组装分配策略 **/
        JSONObject workerPolicyConfig = stepConfigObj.getJSONObject("workerPolicyConfig");
        if (MapUtils.isNotEmpty(workerPolicyConfig)) {
            JSONArray policyList = workerPolicyConfig.getJSONArray("policyList");
            if (CollectionUtils.isNotEmpty(policyList)) {
                List<ProcessStepWorkerPolicyVo> workerPolicyList = new ArrayList<>();
                for (int k = 0; k < policyList.size(); k++) {
                    JSONObject policyObj = policyList.getJSONObject(k);
                    if (!"1".equals(policyObj.getString("isChecked"))) {
                        continue;
                    }
                    ProcessStepWorkerPolicyVo processStepWorkerPolicyVo = new ProcessStepWorkerPolicyVo();
                    processStepWorkerPolicyVo.setProcessUuid(processStepVo.getProcessUuid());
                    processStepWorkerPolicyVo.setProcessStepUuid(processStepVo.getUuid());
                    processStepWorkerPolicyVo.setPolicy(policyObj.getString("type"));
                    processStepWorkerPolicyVo.setSort(k + 1);
                    processStepWorkerPolicyVo.setConfig(policyObj.getString("config"));
                    workerPolicyList.add(processStepWorkerPolicyVo);
                }
                processStepVo.setWorkerPolicyList(workerPolicyList);
            }
        }

        JSONArray tagList = stepConfigObj.getJSONArray("tagList");
        if (CollectionUtils.isNotEmpty(tagList)) {
            processStepVo.setTagList(tagList.toJavaList(String.class));
        }
        // 保存表单场景
        String formSceneUuid = stepConfigObj.getString("formSceneUuid");
        if (StringUtils.isNotBlank(formSceneUuid)) {
            processStepVo.setFormSceneUuid(formSceneUuid);
        }
    }

    @Override
    public void updateProcessTaskStepUserAndWorker(Long processTaskId, Long processTaskStepId) {

    }

    @SuppressWarnings("serial")
    @Override
    public JSONObject makeupConfig(JSONObject configObj) {
        if (configObj == null) {
            configObj = new JSONObject();
        }
        JSONObject resultObj = new JSONObject();

        /** 授权 **/
        IOperationType[] stepActions = {
                ProcessTaskStepOperationType.STEP_VIEW,
                ProcessTaskStepOperationType.STEP_TRANSFER
        };
        JSONArray authorityList = configObj.getJSONArray("authorityList");
        JSONArray authorityArray = ProcessConfigUtil.regulateAuthorityList(authorityList, stepActions);
        resultObj.put("authorityList", authorityArray);

        /* 按钮映射 **/
        IOperationType[] stepButtons = {
                ProcessTaskStepOperationType.STEP_COMPLETE,
                ProcessTaskStepOperationType.STEP_BACK,
                ProcessTaskOperationType.PROCESSTASK_TRANSFER,
                ProcessTaskStepOperationType.STEP_ACCEPT
        };
        JSONArray customButtonList = configObj.getJSONArray("customButtonList");
        JSONArray customButtonArray = ProcessConfigUtil.regulateCustomButtonList(customButtonList, stepButtons);
        resultObj.put("customButtonList", customButtonArray);

        /* 状态映射列表 **/
        JSONArray customStatusList = configObj.getJSONArray("customStatusList");
        JSONArray customStatusArray = ProcessConfigUtil.regulateCustomStatusList(customStatusList);
        resultObj.put("customStatusList", customStatusArray);

        /* 可替换文本列表 **/
        resultObj.put("replaceableTextList", ProcessConfigUtil.regulateReplaceableTextList(configObj.getJSONArray("replaceableTextList")));
        return resultObj;
    }

    @Override
    public JSONObject regulateProcessStepConfig(JSONObject configObj) {
        if (configObj == null) {
            configObj = new JSONObject();
        }
        JSONObject resultObj = new JSONObject();

        /* 授权 **/
        IOperationType[] stepActions = {
                ProcessTaskStepOperationType.STEP_VIEW,
                ProcessTaskStepOperationType.STEP_TRANSFER
        };
        JSONArray authorityList = null;
        Integer enableAuthority = configObj.getInteger("enableAuthority");
        if (Objects.equals(enableAuthority, 1)) {
            authorityList = configObj.getJSONArray("authorityList");
        } else {
            enableAuthority = 0;
        }
        resultObj.put("enableAuthority", enableAuthority);
        JSONArray authorityArray = ProcessConfigUtil.regulateAuthorityList(authorityList, stepActions);
        resultObj.put("authorityList", authorityArray);

        /** 通知 **/
        JSONObject notifyPolicyConfig = configObj.getJSONObject("notifyPolicyConfig");
        INotifyServiceCrossoverService notifyServiceCrossoverService = CrossoverServiceFactory.getApi(INotifyServiceCrossoverService.class);
        InvokeNotifyPolicyConfigVo invokeNotifyPolicyConfigVo = notifyServiceCrossoverService.regulateNotifyPolicyConfig(notifyPolicyConfig, CmdbSyncNotifyHandler.class);
        resultObj.put("notifyPolicyConfig", invokeNotifyPolicyConfigVo);

        /** 动作 **/
        JSONObject actionConfig = configObj.getJSONObject("actionConfig");
        ActionConfigVo actionConfigVo = JSONObject.toJavaObject(actionConfig, ActionConfigVo.class);
        if (actionConfigVo == null) {
            actionConfigVo = new ActionConfigVo();
        }
        actionConfigVo.setHandler(CmdbSyncNotifyHandler.class.getName());
        resultObj.put("actionConfig", actionConfigVo);

        JSONArray customButtonList = configObj.getJSONArray("customButtonList");
        /* 按钮映射列表 **/
        IOperationType[] stepButtons = {
                ProcessTaskStepOperationType.STEP_COMPLETE,
                ProcessTaskStepOperationType.STEP_BACK,
                ProcessTaskOperationType.PROCESSTASK_TRANSFER,
                ProcessTaskStepOperationType.STEP_ACCEPT
        };

        JSONArray customButtonArray = ProcessConfigUtil.regulateCustomButtonList(customButtonList, stepButtons);
        resultObj.put("customButtonList", customButtonArray);
        /* 状态映射列表 **/
        JSONArray customStatusList = configObj.getJSONArray("customStatusList");
        JSONArray customStatusArray = ProcessConfigUtil.regulateCustomStatusList(customStatusList);
        resultObj.put("customStatusList", customStatusArray);

        /* 可替换文本列表 **/
        resultObj.put("replaceableTextList", ProcessConfigUtil.regulateReplaceableTextList(configObj.getJSONArray("replaceableTextList")));

        /* 自动化配置 **/
        JSONObject ciEntityConfig = configObj.getJSONObject("ciEntityConfig");
        CiEntitySyncVo ciEntitySyncVo = regulateCiEntityConfig(ciEntityConfig);
        resultObj.put("ciEntityConfig", ciEntitySyncVo);

        /* 分配处理人 **/
        JSONObject workerPolicyConfig = configObj.getJSONObject("workerPolicyConfig");
        JSONObject workerPolicyObj = ProcessConfigUtil.regulateWorkerPolicyConfig(workerPolicyConfig);
        resultObj.put("workerPolicyConfig", workerPolicyObj);

        JSONObject simpleSettings = ProcessConfigUtil.regulateSimpleSettings(configObj);
        resultObj.putAll(simpleSettings);
        /* 表单场景 **/
        String formSceneUuid = configObj.getString("formSceneUuid");
        String formSceneName = configObj.getString("formSceneName");
        resultObj.put("formSceneUuid", formSceneUuid == null ? "" : formSceneUuid);
        resultObj.put("formSceneName", formSceneName == null ? "" : formSceneName);
        return resultObj;
    }

    private CiEntitySyncVo regulateCiEntityConfig(JSONObject ciEntityConfig) {
        CiEntitySyncVo ciEntitySyncVo = new CiEntitySyncVo();
        if (ciEntityConfig != null) {
            ciEntitySyncVo = ciEntityConfig.toJavaObject(CiEntitySyncVo.class);
        }
        // 失败策略
        String failPolicy = ciEntitySyncVo.getFailPolicy();
        if (failPolicy == null) {
            if (ciEntityConfig != null) {
                logger.warn("ciEntityConfig.failPolicy is null");
                throw new CiEntityConfigIllegalException("ciEntityConfig.failPolicy is null");
            }
            ciEntitySyncVo.setFailPolicy(StringUtils.EMPTY);
        }
        // 回退步骤重新同步
        Integer rerunStepToSync = ciEntitySyncVo.getRerunStepToSync();
        if (rerunStepToSync == null) {
            if (ciEntityConfig != null) {
                logger.warn("ciEntityConfig.rerunStepToSync is null");
                throw new CiEntityConfigIllegalException("ciEntityConfig.rerunStepToSync is null");
            }
            ciEntitySyncVo.setRerunStepToSync(0);
        }
        List<CiEntitySyncConfigVo> configList = ciEntitySyncVo.getConfigList();
        if (CollectionUtils.isEmpty(configList)) {
            if (ciEntityConfig != null) {
                logger.warn("ciEntityConfig.configList is null");
                throw new CiEntityConfigIllegalException("ciEntityConfig.configList is null");
            }
            return ciEntitySyncVo;
        }
        Iterator<CiEntitySyncConfigVo> iterator = configList.iterator();
        while (iterator.hasNext()) {
            CiEntitySyncConfigVo configObj = iterator.next();
            if (configObj == null) {
                iterator.remove();
                continue;
            }
            if (configObj.getId() != null) {
                logger.warn("ciEntityConfig.configList[x].id is not null");
                configObj.setId(null);
            }
            String ciName = configObj.getCiName();
            if (StringUtils.isBlank(ciName)) {
                logger.warn("ciEntityConfig.configList[x].ciName is null");
                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[x].ciName is null");
            }
            String name = ciName;
            String ciLabel = configObj.getCiLabel();
            if (StringUtils.isBlank(ciLabel)) {
                logger.warn("ciEntityConfig.configList[" + name + "].ciLabel is null");
                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].ciLabel is null");
            }
            name += "(" + ciLabel + ")";
            if (StringUtils.isBlank(configObj.getUuid())) {
                logger.warn("ciEntityConfig.configList[" + name + "].uuid is null");
                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].uuid is null");
            }
            if (configObj.getCiId() == null) {
                logger.warn("ciEntityConfig.configList[" + name + "].ciId is null");
                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].ciId is null");
            }
            if (StringUtils.isBlank(configObj.getCiIcon())) {
                logger.warn("ciEntityConfig.configList[" + name + "].ciIcon is null");
                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].ciIcon is null");
            }
            String createPolicy = configObj.getCreatePolicy();
            if (StringUtils.isBlank(createPolicy)) {
                logger.warn("ciEntityConfig.configList[" + name + "].createPolicy is null");
                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].createPolicy is null");
            }
            CiEntitySyncBatchDataSourceVo batchDataSource = configObj.getBatchDataSource();
            if (Objects.equals(createPolicy, "single")) {
                if (batchDataSource != null) {
                    if (StringUtils.isNotBlank(batchDataSource.getAttributeUuid())) {
                        logger.warn("ciEntityConfig.configList[" + name + "].batchDataSource.attributeUuid is not null");
                    }
                    List<CiEntitySyncFilterVo> filterList = batchDataSource.getFilterList();
                    if (CollectionUtils.isNotEmpty(filterList)) {
                        logger.warn("ciEntityConfig.configList[" + name + "].batchDataSource.filterList is not null");
                    }
                }
            } else if (Objects.equals(createPolicy, "batch")) {
                if (batchDataSource == null) {
                    logger.warn("createPolicy = batch, ciEntityConfig.configList[" + name + "].batchDataSource is null");
                    throw new CiEntityConfigIllegalException("createPolicy = batch, ciEntityConfig.configList[" + name + "].batchDataSource is null");
                }
                if (StringUtils.isBlank(batchDataSource.getAttributeUuid())) {
                    logger.warn("ciEntityConfig.configList[" + name + "].batchDataSource.attributeUuid is null");
                    throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].batchDataSource.attributeUuid is null");
                }
                String type = batchDataSource.getType();
                if (StringUtils.isBlank(type)) {
                    logger.warn("createPolicy = batch, ciEntityConfig.configList[" + name + "].batchDataSource.type is null");
                    throw new CiEntityConfigIllegalException("createPolicy = batch, ciEntityConfig.configList[" + name + "].batchDataSource.type is null");
                } else if (!Objects.equals(type, "formSubassemblyComponent") && !Objects.equals(type, "formTableComponent")) {
                    logger.warn("createPolicy = batch, ciEntityConfig.configList[" + name + "].batchDataSource.type = " + type + " is not valid");
                    throw new CiEntityConfigIllegalException("createPolicy = batch, ciEntityConfig.configList[" + name + "].batchDataSource.type = " + type + " is not valid");
                }
                List<CiEntitySyncFilterVo> filterList = batchDataSource.getFilterList();
                if (CollectionUtils.isNotEmpty(filterList)) {
                    Iterator<CiEntitySyncFilterVo> filterIterator = filterList.iterator();
                    while (filterIterator.hasNext()) {
                        CiEntitySyncFilterVo filterVo = filterIterator.next();
                        if (filterVo == null) {
                            logger.warn("ciEntityConfig.configList[" + name + "].batchDataSource.filterList[y] is null");
                            filterIterator.remove();
                            continue;
                        }
                        if (StringUtils.isBlank(filterVo.getColumn())) {
                            logger.warn("ciEntityConfig.configList[" + name + "].batchDataSource.filterList[y].column is null");
                            throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].batchDataSource.filterList[y].column is null");
                        }
                        if (StringUtils.isBlank(filterVo.getExpression())) {
                            logger.warn("ciEntityConfig.configList[" + name + "].batchDataSource.filterList[y].expression is null");
                            throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].batchDataSource.filterList[y].expression is null");
                        }
                        if (StringUtils.isBlank(filterVo.getValue())) {
                            logger.warn("ciEntityConfig.configList[" + name + "].batchDataSource.filterList[y].value is null");
                            throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].batchDataSource.filterList[y].value is null");
                        }
                    }
                }
            }

            List<CiEntitySyncMappingVo> mappingList = configObj.getMappingList();
            if (CollectionUtils.isEmpty(mappingList)) {
                logger.warn("ciEntityConfig.configList[" + name + "].mappingList is null");
                continue;
            }
            Iterator<CiEntitySyncMappingVo> mappingIterator = mappingList.iterator();
            while (mappingIterator.hasNext()) {
                CiEntitySyncMappingVo mappingVo = mappingIterator.next();
                if (mappingVo == null) {
                    logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y] is null");
                    mappingIterator.remove();
                    continue;
                }
                if (StringUtils.isBlank(mappingVo.getKey())) {
                    logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].key is null");
                    throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].key is null");
                }
                String mappingMode = mappingVo.getMappingMode();
                if (StringUtils.isBlank(mappingMode)) {
                    logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].mappingMode is null");
                    throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].mappingMode is null");
                }
                JSONArray valueList = mappingVo.getValueList();
                List<CiEntitySyncFilterVo> filterList = mappingVo.getFilterList();
                if (Objects.equals(mappingMode, "formSubassemblyComponent")) {
                    if (CollectionUtils.isEmpty(valueList)) {
                        logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].valueList is null");
                        throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].valueList is null");
                    }
                    if (valueList.get(0) == null) {
                        logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].valueList[0] is null");
                        throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].valueList[0] is null");
                    }
                    if (CollectionUtils.isNotEmpty(filterList)) {
                        Iterator<CiEntitySyncFilterVo> filterIterator = filterList.iterator();
                        while (filterIterator.hasNext()) {
                            CiEntitySyncFilterVo filterVo = filterIterator.next();
                            if (filterVo == null) {
                                logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z] is null");
                                filterIterator.remove();
                                continue;
                            }
                            if (StringUtils.isBlank(filterVo.getColumn())) {
                                logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].column is null");
                                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].column is null");
                            }
                            if (StringUtils.isBlank(filterVo.getExpression())) {
                                logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].expression is null");
                                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].expression is null");
                            }
                            if (StringUtils.isBlank(filterVo.getValue())) {
                                logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].value is null");
                                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].value is null");
                            }
                        }
                    }
                } else if (Objects.equals(mappingMode, "formTableComponent")) {
                    if (CollectionUtils.isEmpty(valueList)) {
                        logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].valueList is null");
                        throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].valueList is null");
                    }
                    if (valueList.get(0) == null) {
                        logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].valueList[0] is null");
                        throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].valueList[0] is null");
                    }
                    if (CollectionUtils.isNotEmpty(filterList)) {
                        Iterator<CiEntitySyncFilterVo> filterIterator = filterList.iterator();
                        while (filterIterator.hasNext()) {
                            CiEntitySyncFilterVo filterVo = filterIterator.next();
                            if (filterVo == null) {
                                logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z] is null");
                                filterIterator.remove();
                                continue;
                            }
                            if (StringUtils.isBlank(filterVo.getColumn())) {
                                logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].column is null");
                                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].column is null");
                            }
                            if (StringUtils.isBlank(filterVo.getExpression())) {
                                logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].expression is null");
                                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].expression is null");
                            }
                            if (StringUtils.isBlank(filterVo.getValue())) {
                                logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].value is null");
                                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].filterList[z].value is null");
                            }
                        }
                    }
                } else if (Objects.equals(mappingMode, "formCommonComponent")) {
                    if (CollectionUtils.isEmpty(valueList)) {
                        logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].valueList is null");
                        throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].valueList is null");
                    }
                    for (int i = 0; i < valueList.size(); i++) {
                        if (valueList.get(i) == null) {
                            logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].valueList[z] is null");
                            throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].valueList[z] is null");
                        }
                    }
                    if (CollectionUtils.isNotEmpty(filterList)) {
                        logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList is not null");
                        mappingVo.setFilterList(null);
                    }
                } else if (Objects.equals(mappingMode, "constant")) {
                    if (CollectionUtils.isNotEmpty(filterList)) {
                        logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList is not null");
                        mappingVo.setFilterList(null);
                    }
                } else if (Objects.equals(mappingMode, "new")) {
                    if (CollectionUtils.isEmpty(valueList)) {
                        logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].valueList is null");
                    } else {
                        if (valueList.get(0) == null) {
                            logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].valueList[0] is null");
                            throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].valueList[0] is null");
                        }
                    }
                    if (CollectionUtils.isNotEmpty(filterList)) {
                        logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].filterList is not null");
                        mappingVo.setFilterList(null);
                    }
                }
            }

//            JSONArray children = configObj.getChildren();
//            if (CollectionUtils.isEmpty(children)) {
//                continue;
//            }
//            for (int i = children.size() - 1; i >= 0; i--) {
//                JSONObject child = children.getJSONObject(i);
//                if (MapUtils.isEmpty(child)) {
//                    logger.warn("ciEntityConfig.configList[" + name + "].children[i] is null");
//                    children.remove(i);
//                    continue;
//                }
//                String ciEntityUuid = child.getString("ciEntityUuid");
//                if (StringUtils.isBlank(ciEntityUuid)) {
//                    logger.warn("ciEntityConfig.configList[" + name + "].children[i].ciEntityUuid is null");
//                    throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].children[i].ciEntityUuid is null");
//                }
//                String ciEntityName = child.getString("ciEntityName");
//                if (StringUtils.isBlank(ciEntityName)) {
//                    logger.warn("ciEntityConfig.configList[" + name + "].children[i].ciEntityName is null");
//                    throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].children[i].ciEntityName is null");
//                }
//                Long ciId = child.getLong("ciId");
//                if (ciId == null) {
//                    logger.warn("ciEntityConfig.configList[" + name + "].children[i].ciId is null");
//                    throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].children[i].ciId is null");
//                }
//            }
        }

        Set<String> usedUuidList = new HashSet<>();
        List<String> allUuidList = configList.stream().map(CiEntitySyncConfigVo::getUuid).collect(Collectors.toList());
        for (CiEntitySyncConfigVo ciEntitySyncConfigVo : configList) {
            String ciName = ciEntitySyncConfigVo.getCiName();
            String name = ciName;
            String ciLabel = ciEntitySyncConfigVo.getCiLabel();
            name += "(" + ciLabel + ")";
//            JSONArray children = ciEntitySyncConfigVo.getChildren();
//            if (CollectionUtils.isNotEmpty(children)) {
//                for (int i = 0; i < children.size(); i++) {
//                    JSONObject child = children.getJSONObject(i);
//                    String ciEntityUuid = child.getString("ciEntityUuid");
//                    if (!allUuidList.contains(ciEntityUuid)) {
//                        logger.warn("ciEntityConfig.configList[" + name + "].children[i].ciEntityUuid = '" + ciEntityUuid + "' is illegal");
//                        throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].children[i].ciEntityUuid = '" + ciEntityUuid + "' is illegal");
//                    }
//                    usedUuidList.add(ciEntityUuid);
//                }
//            }
            List<CiEntitySyncMappingVo> mappingList = ciEntitySyncConfigVo.getMappingList();
            if (CollectionUtils.isNotEmpty(mappingList)) {
                for (CiEntitySyncMappingVo mapping : mappingList) {
                    if (Objects.equals(mapping.getMappingMode(), "new")) {
                        JSONArray valueList = mapping.getValueList();
                        for (int i = 0; i < valueList.size(); i++) {
                            JSONObject valueObj = valueList.getJSONObject(i);
                            String type = valueObj.getString("type");
                            if (!Objects.equals(type, "new")) {
                                continue;
                            }
                            String ciEntityUuid = valueObj.getString("ciEntityUuid");
                            if (!allUuidList.contains(ciEntityUuid)) {
                                logger.warn("ciEntityConfig.configList[" + name + "].mappingList[y].valueList[z].ciEntityUuid = '" + ciEntityUuid + "' is illegal");
                                throw new CiEntityConfigIllegalException("ciEntityConfig.configList[" + name + "].mappingList[y].valueList[z].ciEntityUuid = '" + ciEntityUuid + "' is illegal");
                            }
                            usedUuidList.add(ciEntityUuid);
                        }
                    }
                }
            }
        }
        allUuidList.removeAll(usedUuidList);
        if (CollectionUtils.isNotEmpty(allUuidList)) {
            for (int i = configList.size() - 1; i >= 0; i--) {
                CiEntitySyncConfigVo ciEntitySyncConfigVo = configList.get(i);
                if (allUuidList.contains(ciEntitySyncConfigVo.getUuid()) && !Objects.equals(ciEntitySyncConfigVo.getIsStart(), 1)) {
                    logger.warn("ciEntitySyncConfig is not used：" + JSONObject.toJSONString(ciEntitySyncConfigVo));
                    configList.remove(i);
                }
            }
        }
        return ciEntitySyncVo;
    }
}
