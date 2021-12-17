/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.cmdb.startup.handler;

import codedriver.framework.cmdb.dto.sync.SyncAuditVo;
import codedriver.framework.cmdb.enums.sync.SyncStatus;
import codedriver.framework.common.config.Config;
import codedriver.framework.startup.IStartup;
import codedriver.module.cmdb.dao.mapper.sync.SyncAuditMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class ResetCiSyncStatusStartupHandler implements IStartup {
    @Resource
    private SyncAuditMapper syncAuditMapper;

    @Override
    public String getName() {
        return "重置自动采集作业状态";
    }

    @Override
    public int sort() {
        return 3;
    }

    @Override
    public void executeForCurrentTenant() {
        List<SyncAuditVo> auditList = syncAuditMapper.getDoingSyncByServerId(Config.SCHEDULE_SERVER_ID);
        if (CollectionUtils.isNotEmpty(auditList)) {
            for (SyncAuditVo audit : auditList) {
                audit.setStatus(SyncStatus.DONE.getValue());
                audit.setError("系统重启，作业终止");
                syncAuditMapper.updateSyncAuditStatus(audit);
            }
        }
    }

    @Override
    public void executeForAllTenant() {

    }
}