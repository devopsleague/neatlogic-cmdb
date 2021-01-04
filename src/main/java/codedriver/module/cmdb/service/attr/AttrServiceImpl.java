package codedriver.module.cmdb.service.attr;

import codedriver.framework.batch.BatchJob;
import codedriver.framework.batch.BatchRunner;
import codedriver.framework.cmdb.constvalue.TransactionActionType;
import codedriver.module.cmdb.dao.mapper.ci.AttrMapper;
import codedriver.module.cmdb.dao.mapper.cientity.AttrEntityMapper;
import codedriver.module.cmdb.dao.mapper.cientity.CiEntityMapper;
import codedriver.module.cmdb.dao.mapper.transaction.TransactionMapper;
import codedriver.module.cmdb.dto.ci.AttrVo;
import codedriver.module.cmdb.dto.cientity.CiEntityVo;
import codedriver.module.cmdb.dto.transaction.AttrEntityTransactionVo;
import codedriver.module.cmdb.dto.transaction.CiEntityTransactionVo;
import codedriver.module.cmdb.dto.transaction.TransactionGroupVo;
import codedriver.module.cmdb.dto.transaction.TransactionVo;
import codedriver.module.cmdb.service.cientity.CiEntityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class AttrServiceImpl implements AttrService {
    private final static Logger logger = LoggerFactory.getLogger(AttrServiceImpl.class);

    @Autowired
    private AttrMapper attrMapper;

    @Autowired
    private AttrEntityMapper attrEntityMapper;

    @Autowired
    private TransactionMapper transactionMapper;

    @Autowired
    private CiEntityMapper ciEntityMapper;

    @Autowired
    private CiEntityService ciEntityService;


    @Override
    @Transactional
    public void deleteAttrById(AttrVo attrVo) {
        //删除所有attrEntity属性值，需要生成事务
        List<CiEntityVo> ciEntityList = ciEntityMapper.getCiEntityByAttrId(attrVo.getId());
        BatchRunner<CiEntityVo> runner = new BatchRunner<>();
        TransactionGroupVo transactionGroupVo = new TransactionGroupVo();
        //并发清理配置项数据，最高并发10个线程
        int parallel = 10;
        runner.execute(ciEntityList, parallel, new BatchJob<CiEntityVo>() {
            @Override
            public void execute(CiEntityVo item) {
                if (item != null) {
                    try {
                        //写入事务
                        TransactionVo transactionVo = new TransactionVo();
                        transactionVo.setCiId(item.getCiId());
                        transactionMapper.insertTransaction(transactionVo);
                        //写入事务分组
                        transactionMapper.insertTransactionGroup(transactionGroupVo.getId(), transactionVo.getId());
                        //写入配置项事务
                        CiEntityTransactionVo ciEntityTransactionVo = new CiEntityTransactionVo();
                        ciEntityTransactionVo.setCiEntityId(item.getId());
                        ciEntityTransactionVo.setCiId(item.getCiId());
                        ciEntityTransactionVo.setTransactionMode(TransactionActionType.UPDATE);
                        ciEntityTransactionVo.setAction(TransactionActionType.UPDATE.getValue());
                        ciEntityTransactionVo.setTransactionId(transactionVo.getId());
                        AttrEntityTransactionVo attrEntityVo = new AttrEntityTransactionVo();
                        attrEntityVo.setAttrId(attrVo.getId());
                        attrEntityVo.setCiEntityId(item.getId());
                        attrEntityVo.setAttrName(attrVo.getName());
                        attrEntityVo.setAttrLabel(attrVo.getLabel());
                        attrEntityVo.setTransactionId(transactionVo.getId());

                        // 保存快照
                        ciEntityService.createSnapshot(ciEntityTransactionVo);

                        //写入配置项事务
                        transactionMapper.insertCiEntityTransaction(ciEntityTransactionVo);

                        // 写入属性事务
                        transactionMapper.insertAttrEntityTransaction(attrEntityVo);

                        //提交事务
                        transactionVo.setCiEntityTransactionVo(ciEntityTransactionVo);
                        ciEntityService.commitTransaction(transactionVo);
                    } catch (Exception ex) {
                        logger.error(ex.getMessage(), ex);
                    }
                }
            }
        });

        attrMapper.deleteAttrById(attrVo.getId());
    }


}
