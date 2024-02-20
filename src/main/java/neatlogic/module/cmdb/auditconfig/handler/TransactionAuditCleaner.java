/*
 * Copyright(c) 2023 NeatLogic Co., Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neatlogic.module.cmdb.auditconfig.handler;

import neatlogic.framework.auditconfig.core.AuditCleanerBase;
import neatlogic.framework.healthcheck.dao.mapper.DatabaseFragmentMapper;
import neatlogic.module.cmdb.dao.mapper.transaction.TransactionMapper;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class TransactionAuditCleaner extends AuditCleanerBase {
    @Resource
    private TransactionMapper transactionMapper;

    @Resource
    private DatabaseFragmentMapper databaseFragmentMapper;

    @Override
    public String getName() {
        return "CMDB-TRANSACTION";
    }

    @Override
    protected void myClean(int dayBefore) {
        transactionMapper.deleteTransactionByDayBefore(dayBefore);
        //databaseFragmentMapper.rebuildTable(TenantContext.get().getDbName(), "cmdb_transaction");
        //databaseFragmentMapper.rebuildTable(TenantContext.get().getDbName(), "cmdb_cientity_transaction");
        //databaseFragmentMapper.rebuildTable(TenantContext.get().getDbName(), "cmdb_transactiongroup");
    }
}
