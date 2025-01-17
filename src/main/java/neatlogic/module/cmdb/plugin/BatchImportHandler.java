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

package neatlogic.module.cmdb.plugin;

import com.alibaba.fastjson.JSONArray;
import neatlogic.framework.asynchronization.thread.NeatLogicThread;
import neatlogic.framework.asynchronization.threadlocal.InputFromContext;
import neatlogic.framework.cmdb.attrvaluehandler.core.AttrValueHandlerFactory;
import neatlogic.framework.cmdb.attrvaluehandler.core.IAttrValueHandler;
import neatlogic.framework.cmdb.dto.batchimport.ImportAuditVo;
import neatlogic.framework.cmdb.dto.ci.AttrVo;
import neatlogic.framework.cmdb.dto.ci.CiViewVo;
import neatlogic.framework.cmdb.dto.ci.CiVo;
import neatlogic.framework.cmdb.dto.ci.RelVo;
import neatlogic.framework.cmdb.dto.cientity.CiEntityVo;
import neatlogic.framework.cmdb.dto.globalattr.GlobalAttrItemVo;
import neatlogic.framework.cmdb.dto.globalattr.GlobalAttrVo;
import neatlogic.framework.cmdb.dto.transaction.CiEntityTransactionVo;
import neatlogic.framework.cmdb.dto.transaction.TransactionGroupVo;
import neatlogic.framework.cmdb.enums.*;
import neatlogic.framework.cmdb.exception.batchimport.*;
import neatlogic.framework.cmdb.exception.ci.CiIsAbstractedException;
import neatlogic.framework.cmdb.exception.ci.CiIsVirtualException;
import neatlogic.framework.cmdb.exception.ci.CiNotFoundException;
import neatlogic.framework.cmdb.exception.ci.CiWithoutAttrRelException;
import neatlogic.framework.cmdb.exception.cientity.AttrEntityValueEmptyException;
import neatlogic.framework.cmdb.exception.cientity.CiEntityNotFoundException;
import neatlogic.framework.cmdb.exception.cientity.RelEntityNotFoundException;
import neatlogic.framework.common.constvalue.InputFrom;
import neatlogic.framework.common.util.FileUtil;
import neatlogic.framework.exception.core.ApiRuntimeException;
import neatlogic.framework.file.dto.FileVo;
import neatlogic.framework.util.$;
import neatlogic.framework.util.UuidUtil;
import neatlogic.module.cmdb.dao.mapper.batchimport.ImportMapper;
import neatlogic.module.cmdb.dao.mapper.ci.CiMapper;
import neatlogic.module.cmdb.dao.mapper.ci.CiViewMapper;
import neatlogic.module.cmdb.dao.mapper.cientity.CiEntityMapper;
import neatlogic.module.cmdb.dao.mapper.globalattr.GlobalAttrMapper;
import neatlogic.module.cmdb.service.ci.CiService;
import neatlogic.module.cmdb.service.cientity.CiEntityService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.usermodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

@Service
public class BatchImportHandler {

    public static final Map<Long, String> importMap = new HashMap<>();
    static Logger logger = LoggerFactory.getLogger(BatchImportHandler.class);

    private static ImportMapper importMapper;

    private static CiEntityMapper ciEntityMapper;

    private static CiService ciService;

    private static CiViewMapper ciViewMapper;

    private static CiEntityService ciEntityService;

    private static GlobalAttrMapper globalAttrMapper;

    private static CiMapper ciMapper;

    private AttrValueHandlerFactory attrValueHandlerFactory;

    @Autowired
    private void setGlobalAttrMapper(GlobalAttrMapper _globalAttrMapper) {
        globalAttrMapper = _globalAttrMapper;
    }

    @Autowired
    private void setCiMapper(CiMapper _ciMapper) {
        ciMapper = _ciMapper;
    }

    @Autowired
    public void setImportMapper(ImportMapper _importMapper) {
        importMapper = _importMapper;
    }

    @Autowired
    public void setCiEntityMapper(CiEntityMapper _ciEntityMapper) {
        ciEntityMapper = _ciEntityMapper;
    }

    @Autowired
    public void setCiService(CiService _ciService) {
        ciService = _ciService;
    }

    @Autowired
    public void setCiViewMapper(CiViewMapper _ciViewMapper) {
        ciViewMapper = _ciViewMapper;
    }

    @Autowired
    public void setCiEntityService(CiEntityService _ciEntityService) {
        ciEntityService = _ciEntityService;
    }

    /*public static void main(String[] a) {
        double num = 1.12123123;
        BigDecimal bd = new BigDecimal(Double.toString(num));
        bd = bd.setScale(4, RoundingMode.HALF_UP);
        String result = bd.stripTrailingZeros().toPlainString();
        System.out.println(result);
    }*/

    private static String getCellContent(Cell cell) {
        String cellContent = "";
        switch (cell.getCellType()) {
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    Date date = cell.getDateCellValue();
                    SimpleDateFormat sFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    cellContent = sFormat.format(date).replace("00:00:00", "");
                } else {
                    BigDecimal bd = new BigDecimal(Double.toString(cell.getNumericCellValue()));
                    bd = bd.setScale(4, RoundingMode.HALF_UP);
                    cellContent = bd.stripTrailingZeros().toPlainString();
                }
                break;
            case STRING:
                cellContent = cell.getStringCellValue();
                break;
            case BOOLEAN:
                cellContent = String.valueOf(cell.getBooleanCellValue());
                break;
            case BLANK:
            case ERROR:
                cellContent = "";
                break;
            case FORMULA:
                cellContent = cell.getCellFormula();
                break;
            default:
                break;
        }
        if (StringUtils.isNotBlank(cellContent)) {
            return cellContent.trim();
        } else {
            return "";
        }
    }

    @Autowired
    public void setAttrValueHandlerFactory(AttrValueHandlerFactory attrValueHandlerFactory) {
        this.attrValueHandlerFactory = attrValueHandlerFactory;
    }


    public static class Importer extends NeatLogicThread {
        private final Long ciId;
        private final String editMode;
        private final FileVo fileVo;
        private final String importUser;
        private final String action;
        private final Map<Long, CiVo> ciMap = new HashMap<>();

        private List<CiEntityVo> getCiEntityBaseInfoByName(Long ciId, String name) {
            if (!ciMap.containsKey(ciId)) {
                ciMap.put(ciId, ciMapper.getCiById(ciId));
            }
            CiVo ciVo = ciMap.get(ciId);
            CiEntityVo ciEntityVo = new CiEntityVo();
            ciEntityVo.setCiId(ciId);
            ciEntityVo.setName(name);
            if (ciVo.getIsVirtual().equals(0)) {
                List<CiVo> list = ciMapper.getDownwardCiListByLR(ciVo.getLft(), ciVo.getRht());
                if (CollectionUtils.isNotEmpty(list)) {
                    ciEntityVo.setIdList(list.stream().map(CiVo::getId).collect(Collectors.toList()));
                    return ciEntityMapper.getCiEntityListByCiIdListAndName(ciEntityVo);
                }
                return null;
            } else {
                return ciEntityMapper.getVirtualCiEntityBaseInfoByName(ciEntityVo);
            }
        }

        public Importer(Long ciId, String action, String editMode, FileVo fileVo, String importUser) {
            super("CIENTITY-EXCEL-IMPORTER");
            this.ciId = ciId;
            this.fileVo = fileVo;
            this.action = action;
            this.editMode = editMode;
            this.importUser = importUser;
        }

        @Override
        public void execute() {
            InputFromContext.init(InputFrom.IMPORT);
            ImportAuditVo importAuditVo = new ImportAuditVo();
            importAuditVo.setCiId(ciId);
            importAuditVo.setImportUser(importUser);
            importAuditVo.setAction(action);
            importAuditVo.setFileId(fileVo.getId());
            importAuditVo.setStatus(ImportStatus.RUNNING.getValue());
            importMapper.insertImportAudit(importAuditVo);

            importMap.put(importAuditVo.getId(), BatchImportStatus.RUNNING.getValue());
            int successCount = 0;
            int failedCount = 0;
            int totalCount = 0;
            /* 用来记录整个表格读取过程中的错误 */
            String error = "";
            Workbook wb = null;
            InputStream in = null;

            try {
                CiVo ciVo = ciService.getCiById(ciId);
                CiViewVo ciViewVo = new CiViewVo();
                ciViewVo.setCiId(ciId);
                ciViewVo.setNeedAlias(1);
                List<CiViewVo> ciViewList = ciViewMapper.getCiViewByCiId(ciViewVo);
                if (ciVo == null) {
                    throw new CiNotFoundException(ciId);
                }
                if (ciVo.getIsAbstract().equals(1)) {
                    throw new CiIsAbstractedException(CiIsAbstractedException.Type.DATA, ciVo.getLabel());
                }
                if (ciVo.getIsVirtual().equals(1)) {
                    throw new CiIsVirtualException(ciVo.getName());
                }
                if (CollectionUtils.isEmpty(ciVo.getAttrList()) && CollectionUtils.isEmpty(ciVo.getRelList())) {
                    throw new CiWithoutAttrRelException(ciVo.getName());
                }

                try {
                    in = FileUtil.getData(fileVo.getPath());
                    //不加这一行会被检测出zip boom
                    ZipSecureFile.setMinInflateRatio(-1.0d);
                    wb = WorkbookFactory.create(in);
                } catch (Exception e) {
                    throw new BatchImportFileFailedException(e.getMessage());
                }

                for (int i = 0; i < wb.getNumberOfSheets(); i++) {
                    Sheet sheet = wb.getSheetAt(i);
                    if (sheet != null) {
                        Row headRow = sheet.getRow(sheet.getFirstRowNum());
                        List<Integer> cellIndex = new ArrayList<>();// 列数
                        Map<Integer, Object> typeMap = new HashMap<>();// 表头列数->表头属性
                        Set<String> checkAttrSet = new HashSet<>();
                        Set<String> requireAttrSet = new HashSet<>();//必填属性或关系
                        try {
                            COLUMN:
                            for (Iterator<Cell> cellIterator = headRow.cellIterator(); cellIterator.hasNext(); ) {
                                Cell cell = cellIterator.next();
                                if (cell != null) {
                                    String content = getCellContent(cell);
                                    if (content.contains("[(")) {
                                        content = content.substring(0, content.indexOf("[("));
                                    }

                                    if (content.equals("id")) {
                                        typeMap.put(cell.getColumnIndex(), "id");
                                        cellIndex.add(cell.getColumnIndex());
                                        continue;
                                    } else if (content.equals("uuid")) {
                                        typeMap.put(cell.getColumnIndex(), "uuid");
                                        cellIndex.add(cell.getColumnIndex());
                                        continue;
                                    }
                                    if (CollectionUtils.isNotEmpty(ciVo.getUniqueAttrIdList())) {
                                        for (Long attrId : ciVo.getUniqueAttrIdList()) {
                                            requireAttrSet.add("attr_" + attrId);
                                        }
                                    }
                                    for (AttrVo attr : ciVo.getAttrList()) {
                                        if (attr.getIsRequired().equals(1)) {
                                            requireAttrSet.add("attr_" + attr.getId());
                                        }
                                    }
                                    for (GlobalAttrVo globalAttr : ciVo.getGlobalAttrList()) {
                                        if (CollectionUtils.isNotEmpty(ciViewList)) {
                                            Optional<CiViewVo> op = ciViewList.stream().filter(d -> d.getType().equals("global") && d.getItemId().equals(globalAttr.getId())).findFirst();
                                            op.ifPresent(viewVo -> globalAttr.setLabel(viewVo.getAlias()));
                                        }
                                        if (content.contains("[" + $.t("term.cmdb.globalattr") + "]") && (globalAttr.getLabel() + "[" + $.t("term.cmdb.globalattr") + "]").equals(content)) {
                                            checkAttrSet.add("global_" + globalAttr.getId());
                                            typeMap.put(cell.getColumnIndex(), globalAttr);
                                            cellIndex.add(cell.getColumnIndex());
                                            continue COLUMN;
                                        }
                                    }
                                    for (AttrVo attr : ciVo.getAttrList()) {
                                        if (CollectionUtils.isNotEmpty(ciViewList)) {
                                            Optional<CiViewVo> op = ciViewList.stream().filter(d -> d.getType().equals("attr") && d.getItemId().equals(attr.getId())).findFirst();
                                            op.ifPresent(viewVo -> attr.setLabel(viewVo.getAlias()));
                                        }
                                        if (attr.getLabel().equals(content)) {
                                            checkAttrSet.add("attr_" + attr.getId());
                                            typeMap.put(cell.getColumnIndex(), attr);
                                            cellIndex.add(cell.getColumnIndex());
                                            continue COLUMN;
                                        }
                                    }
                                    for (RelVo rel : ciVo.getRelList()) {
                                        if ((rel.getDirection().equals(RelDirectionType.FROM.getValue()) && rel.getToIsRequired().equals(1)) ||
                                                (rel.getDirection().equals(RelDirectionType.TO.getValue()) && rel.getFromIsRequired().equals(1))) {
                                            requireAttrSet.add("rel_" + rel.getDirection() + rel.getId());
                                        }
                                    }
                                    for (RelVo rel : ciVo.getRelList()) {
                                        if (CollectionUtils.isNotEmpty(ciViewList)) {
                                            Optional<CiViewVo> op = ciViewList.stream().filter(d -> d.getType().startsWith("rel") && d.getItemId().equals(rel.getId())).findFirst();
                                            if (op.isPresent()) {
                                                CiViewVo view = op.get();
                                                if (view.getType().equals("relfrom")) {
                                                    rel.setToLabel(view.getAlias());
                                                } else if (view.getType().equals("relto")) {
                                                    rel.setFromLabel(view.getAlias());
                                                }
                                            }
                                        }
                                        if ((rel.getDirection().equals(RelDirectionType.FROM.getValue()) && rel.getToLabel().equals(content)) ||
                                                (rel.getDirection().equals(RelDirectionType.TO.getValue()) && rel.getFromLabel().equals(content))) {
                                            checkAttrSet.add("rel_" + rel.getDirection() + rel.getId());
                                            typeMap.put(cell.getColumnIndex(), rel);
                                            cellIndex.add(cell.getColumnIndex());
                                            continue COLUMN;
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            throw new BatchImportAnalyzeHeaderFailedException(e.getMessage());
                        }
                        /*
                          【只添加】与【添加&更新】模式下，不能缺少必填属性列和唯一属性列
                          【只更新】且【全局更新】模式下，不能缺少必填属性列
                         */
                        List<String> lostColumns = new ArrayList<>();
                        if (action.equals("all") || action.equals("append")) {
                            if (CollectionUtils.isNotEmpty(ciVo.getUniqueAttrIdList())) {
                                for (Long attrId : ciVo.getUniqueAttrIdList()) {
                                    if (!checkAttrSet.contains("attr_" + attrId)) {
                                        lostColumns.add(ciVo.getAttrById(attrId).getLabel());
                                    }
                                }
                            }
                        }
                        if (action.equals("all") || action.equals("append")
                                || (action.equals("update") && editMode.equals(EditModeType.GLOBAL.getValue()))) {
                            for (AttrVo attr : ciVo.getAttrList()) {
                                if (attr.getIsRequired().equals(1)
                                        && !checkAttrSet.contains("attr_" + attr.getId())) {
                                    lostColumns.add(attr.getLabel());
                                }
                            }
                            for (RelVo rel : ciVo.getRelList()) {
                                if (rel.getDirection().equals(RelDirectionType.FROM.getValue())) {
                                    if (!checkAttrSet.contains("rel_" + rel.getDirection() + rel.getId())
                                            && rel.getToIsRequired().equals(1)) {
                                        lostColumns.add(rel.getToLabel());
                                    }
                                } else {
                                    if (!checkAttrSet.contains("rel_" + rel.getDirection() + rel.getId())
                                            && rel.getFromIsRequired().equals(1)) {
                                        lostColumns.add(rel.getFromLabel());
                                    }
                                }
                            }
                        }
                        if (CollectionUtils.isNotEmpty(lostColumns)) {
                            error = "<b class=\"text-danger\">导入模版缺少必要的属性：" + Arrays.toString(lostColumns.toArray())
                                    + "</b></br>";
                            totalCount = sheet.getPhysicalNumberOfRows() - 1;
                            failedCount = totalCount;
                        }
                        if (StringUtils.isBlank(error)) {
                            TransactionGroupVo transactionGroupVo = new TransactionGroupVo();
                            totalCount += sheet.getLastRowNum();
                            for (int r = sheet.getFirstRowNum() + 1; r <= sheet.getLastRowNum(); r++) {
                                /* 用来详细记录每一行每一列的错误信息，使用LinkedHashMap是为了计算列数 */
                                Map<Integer, String> errorMsgMap = new LinkedHashMap<>();
                                /* 用来记录每一行最后保存时的错误 */
                                Map<Integer, String> rowError = new LinkedHashMap<>();
                                try {
                                    Row row = sheet.getRow(r);
                                    if (row != null) {
                                        Long ciEntityId = null;
                                        String ciEntityUuid = null;// 记录下表格中的ciEntityId，用于判断配置项是否存在
                                        CiEntityTransactionVo ciEntityTransactionVo = new CiEntityTransactionVo();
                                        ciEntityTransactionVo.setAllowCommit(true);
                                        ciEntityTransactionVo.setCiId(ciId);
                                        ciEntityTransactionVo.setEditMode(editMode);
                                        //先遍历所有cell检查是否空行，如果是空行直接跳过
                                        boolean isEmptyRow = true;
                                        for (Integer index : cellIndex) {
                                            Cell cell = row.getCell(index);
                                            if (cell != null && StringUtils.isNotBlank(getCellContent(cell))) {
                                                isEmptyRow = false;
                                                break;
                                            }
                                        }
                                        if (isEmptyRow) {
                                            continue;
                                        }
                                        for (Integer index : cellIndex) {
                                            Cell cell = row.getCell(index);
                                            if (cell != null) {
                                                String content = getCellContent(cell);
                                                Object header = typeMap.get(index);
                                                if (header instanceof String) {
                                                    if (header.equals("id")) {// 表示拿到的是ID列
                                                        if (StringUtils.isNotBlank(content)) {
                                                            try {
                                                                ciEntityId = Long.parseLong(content);
                                                                ciEntityTransactionVo.setCiEntityId(ciEntityId);
                                                            } catch (Exception e) {
                                                                throw new BatchImportCanNotGetResourceIdException(e.getMessage());
                                                            }
                                                        }
                                                    } else if (header.equals("uuid")) {
                                                        if (StringUtils.isNotBlank(content)) {
                                                            ciEntityUuid = content.trim();
                                                            if (ciEntityUuid.length() != 32) {
                                                                throw new BatchImportUuidLengthIsInvalidException();
                                                            }
                                                        } else {
                                                            ciEntityUuid = UuidUtil.randomUuid();
                                                        }
                                                        ciEntityTransactionVo.setCiEntityUuid(ciEntityUuid);
                                                    }
                                                } else if (header instanceof GlobalAttrVo) {
                                                    GlobalAttrVo globalAttr = (GlobalAttrVo) header;
                                                    JSONArray valueList = new JSONArray();
                                                    if (StringUtils.isNotBlank(content)) {
                                                        for (String c : content.split(",")) {
                                                            if (StringUtils.isNotBlank(c)) {
                                                                GlobalAttrItemVo attrItem = new GlobalAttrItemVo();
                                                                attrItem.setAttrId(globalAttr.getId());
                                                                attrItem.setValue(c);
                                                                List<GlobalAttrItemVo> globalAttrItemList = globalAttrMapper.searchGlobalAttrItem(attrItem);
                                                                // Optional<GlobalAttrItemVo> op = globalAttr.getItemList().stream().filter(d -> d.getValue().equalsIgnoreCase(c)).findFirst();
                                                                //op.ifPresent(valueList::add);
                                                                valueList.addAll(globalAttrItemList);
                                                            }
                                                        }
                                                    }
                                                    if (CollectionUtils.isNotEmpty(valueList)) {
                                                        ciEntityTransactionVo.addGlobalAttrEntityData(globalAttr, valueList);
                                                    }
                                                } else if (header instanceof AttrVo) {// 如果是属性
                                                    AttrVo attr = (AttrVo) header;
                                                    JSONArray valueList = new JSONArray();
                                                    if (requireAttrSet.contains("attr_" + attr.getId()) && StringUtils.isBlank(content)) {
                                                        throw new AttrEntityValueEmptyException(attr.getLabel());
                                                    }
                                                    //如果是引用类型需要先转换成目标配置项的id
                                                    //FIXME 可能需要替换使用唯一规则获取配置项信息
                                                    if (attr.getTargetCiId() != null) {
                                                        for (String c : content.split(",")) {
                                                            if (StringUtils.isNotBlank(c)) {
                                                                c = c.trim();
                                                                List<CiEntityVo> targetCiEntityList = getCiEntityBaseInfoByName(attr.getTargetCiId(), c);
                                                                if (CollectionUtils.isNotEmpty(targetCiEntityList)) {
                                                                    valueList.addAll(targetCiEntityList.stream().map(CiEntityVo::getId).distinct().collect(Collectors.toList()));
                                                                } else {
                                                                    throw new CiEntityNotFoundException(c);
                                                                }
                                                            }
                                                        }
                                                    } else {
                                                        IAttrValueHandler handler = AttrValueHandlerFactory.getHandler(attr.getType());
                                                        if (handler != null) {
                                                            Object newContent = handler.transferValueListToInput(attr, content);
                                                            if (newContent instanceof JSONArray) {
                                                                valueList.addAll((JSONArray) newContent);
                                                            } else {
                                                                valueList.add(content);
                                                            }
                                                        } else {
                                                            valueList.add(content);
                                                        }
                                                    }
                                                    ciEntityTransactionVo.addAttrEntityData(attr, valueList);

                                                } else if (header instanceof RelVo) {
                                                    RelVo rel = (RelVo) header;
                                                    if (requireAttrSet.contains("rel_" + rel.getDirection() + rel.getId()) && StringUtils.isBlank(content)) {
                                                        throw new RelEntityNotFoundException(rel.getDirection().equals(RelDirectionType.FROM.getValue()) ? rel.getToLabel() : rel.getFromLabel());
                                                    }
                                                    for (String c : content.split(",")) {
                                                        if (StringUtils.isNotBlank(c)) {
                                                            c = c.trim();
                                                            List<CiEntityVo> targetCiEntityList = getCiEntityBaseInfoByName(rel.getDirection().equals(RelDirectionType.FROM.getValue()) ? rel.getToCiId() : rel.getFromCiId(), c);
                                                            if (CollectionUtils.isNotEmpty(targetCiEntityList)) {
                                                                for (CiEntityVo entity : targetCiEntityList) {
                                                                    ciEntityTransactionVo.addRelEntityData(rel, rel.getDirection(), ciId, entity.getId());
                                                                }
                                                            } else {
                                                                throw new CiEntityNotFoundException(c);
                                                            }
                                                        }
                                                    }
                                                }
                                            } else {
                                                Object header = typeMap.get(index);
                                                if (header instanceof AttrVo) {// 如果是属性
                                                    AttrVo attr = (AttrVo) header;
                                                    if (requireAttrSet.contains("attr_" + attr.getId())) {
                                                        throw new AttrEntityValueEmptyException(attr.getLabel());
                                                    }
                                                } else if (header instanceof RelVo) {
                                                    RelVo rel = (RelVo) header;
                                                    if (requireAttrSet.contains("rel_" + rel.getDirection() + rel.getId())) {
                                                        throw new RelEntityNotFoundException(rel.getDirection().equals(RelDirectionType.FROM.getValue()) ? rel.getToLabel() : rel.getFromLabel());
                                                    }
                                                }
                                            }
                                        }

                                        try {
                                            if (action.equals("append") && ciEntityId == null) {
                                                ciEntityTransactionVo.setAction(TransactionActionType.INSERT.getValue());
                                                ciEntityService.saveCiEntity(ciEntityTransactionVo, transactionGroupVo);
                                                successCount += 1;
                                            } else if (action.equals("update") && ciEntityId != null) {
                                                ciEntityTransactionVo.setAction(TransactionActionType.UPDATE.getValue());
                                                ciEntityService.saveCiEntity(ciEntityTransactionVo, transactionGroupVo);
                                                successCount += 1;
                                            } else if (action.equals("all")) {
                                                if (ciEntityId != null) {
                                                    ciEntityTransactionVo.setAction(TransactionActionType.UPDATE.getValue());
                                                } else {
                                                    ciEntityTransactionVo.setAction(TransactionActionType.INSERT.getValue());
                                                }
                                                ciEntityService.saveCiEntity(ciEntityTransactionVo);
                                                successCount += 1;
                                            } else {
                                                throw new BatchImportMouldIsMustException();
                                            }
                                        } catch (Exception e) {
                                            failedCount += 1;
                                            logger.error(e.getMessage(), e);
                                            rowError.put(r, e.getMessage());
                                        }
                                    }
                                } catch (Exception e) {
                                    failedCount += 1;
                                    logger.error(e.getMessage(), e);
                                    rowError.put(r, e.getMessage());
                                } finally {
                                    // String err = "";
                                    List<Integer> columnList = new ArrayList<>();
                                    List<String> errorMsgList = new ArrayList<>();
                                    for (Entry<Integer, String> _err : errorMsgMap.entrySet()) {
                                        columnList.add(_err.getKey());
                                        errorMsgList.add(_err.getValue());
                                    }
                                    String errMsg = Arrays.toString(errorMsgList.toArray());
                                    String newerrMsg = errMsg.replace(',', ';');
                                    if (CollectionUtils.isNotEmpty(columnList)) {
                                        error += "</br><b class=\"text-danger\">第" + r + "行第"
                                                + Arrays.toString(columnList.toArray()) + "列</b>：" + newerrMsg;
                                    }

                                    if (MapUtils.isNotEmpty(rowError)) {
                                        for (Entry<Integer, String> entry : rowError.entrySet()) {
                                            error += "</br><b class=\"text-danger\">第" + entry.getKey() + "行："
                                                    + entry.getValue() + "</b>";
                                        }
                                    }

                                    importAuditVo.setSuccessCount(successCount);
                                    importAuditVo.setFailedCount(failedCount);
                                    importAuditVo.setTotalCount(totalCount);
                                    importAuditVo.setError(StringUtils.isNotBlank(error) ? error : null);
                                    importMapper.updateImportAuditTemporary(importAuditVo);

                                }
                            }
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                importAuditVo.setError((StringUtils.isBlank(importAuditVo.getError()) ? ""
                        : importAuditVo.getError() + "<br>") + (e instanceof ApiRuntimeException ? ((ApiRuntimeException) e).getMessage() : e.getMessage()));
                logger.error(e.getMessage(), e);
            } finally {
                try {
                    if (wb != null) {
                        wb.close();
                    }
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
                if (StringUtils.isNotBlank(error)) {
                    importAuditVo.setError(error);
                }
                if (BatchImportStatus.STOPPED.getValue().equals(importMap.get(importAuditVo.getId()))) {
                    importAuditVo.setError((("".equals(importAuditVo.getError()) || importAuditVo.getError() == null)
                            ? "" : importAuditVo.getError() + "<br>") + "<b class=\"text-danger\">导入已停止</b>。");
                }
                if (failedCount > 0) {
                    importAuditVo.setStatus(ImportStatus.FAILED.getValue());
                } else {
                    importAuditVo.setStatus(ImportStatus.SUCCESS.getValue());
                }
                importAuditVo.setSuccessCount(successCount);
                importAuditVo.setFailedCount(failedCount);
                importAuditVo.setTotalCount(totalCount);
                importMapper.updateImportAudit(importAuditVo);
                importMap.replace(importAuditVo.getId(), BatchImportStatus.SUCCEED.getValue());
                importMap.remove(importAuditVo.getId());
            }
        }
    }


    public static void stopImportById(Long id) {
        importMap.replace(id, BatchImportStatus.STOPPED.getValue());
    }
}
