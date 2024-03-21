/*
 * Copyright (C) 2024  深圳极向量科技有限公司 All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package neatlogic.module.cmdb.matrix.handler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.cmdb.dto.customview.CustomViewVo;
import neatlogic.framework.cmdb.exception.customview.CustomViewNotFoundException;
import neatlogic.framework.dependency.core.DependencyManager;
import neatlogic.framework.exception.type.ParamNotExistsException;
import neatlogic.framework.matrix.core.MatrixDataSourceHandlerBase;
import neatlogic.framework.matrix.dto.MatrixAttributeVo;
import neatlogic.framework.matrix.dto.MatrixCmdbCustomViewVo;
import neatlogic.framework.matrix.dto.MatrixDataVo;
import neatlogic.framework.matrix.dto.MatrixVo;
import neatlogic.framework.matrix.exception.MatrixCmdbCustomViewNotFoundException;
import neatlogic.framework.util.UuidUtil;
import neatlogic.module.cmdb.dao.mapper.customview.CustomViewMapper;
import neatlogic.module.cmdb.matrix.constvalue.MatrixType;
import neatlogic.module.cmdb.service.customview.CustomViewDataService;
import neatlogic.module.cmdb.workerdispatcher.exception.CmdbDispatcherDispatchFailedException;
import neatlogic.module.framework.dependency.handler.CiAttr2MatrixAttrDependencyHandler;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

@Component
public class CmdbCustomViewDataSourceHandler extends MatrixDataSourceHandlerBase {

    private final static Logger logger = LoggerFactory.getLogger(CmdbCustomViewDataSourceHandler.class);

    @Resource
    private CustomViewMapper customViewMapper;

    @Resource
    private CustomViewDataService customViewDataService;

    @Override
    public String getHandler() {
        return MatrixType.CMDBCUSTOMVIEW.getValue();
    }

    @Override
    protected boolean mySaveMatrix(MatrixVo matrixVo) throws Exception {
        Long customViewId = matrixVo.getCustomViewId();
        if (customViewId != null) {
            throw new ParamNotExistsException("customViewId");
        }
        CustomViewVo customView = customViewMapper.getCustomViewById(customViewId);
        if (customView == null) {
            throw new CmdbDispatcherDispatchFailedException(customViewId);
        }
        JSONObject config = matrixVo.getConfig();
        if (MapUtils.isEmpty(config)) {
            throw new ParamNotExistsException("config");
        }
        JSONArray showAttributeLabelArray = config.getJSONArray("showAttributeLabelList");
        if (CollectionUtils.isEmpty(showAttributeLabelArray)) {
            throw new ParamNotExistsException("config.showAttributeLabelList");
        }
        Map<String, String> oldShowAttributeUuidMap = new HashMap<>();
        MatrixCmdbCustomViewVo oldMatrixCmdbCustomViewVo = matrixMapper.getMatrixCmdbCustomViewByMatrixUuid(matrixVo.getUuid());
        if (oldMatrixCmdbCustomViewVo != null) {
            if (customViewId.equals(oldMatrixCmdbCustomViewVo.getCustomViewId())) {
                JSONObject oldConfig = oldMatrixCmdbCustomViewVo.getConfig();
                if (MapUtils.isNotEmpty(oldConfig)) {
                    JSONArray oldShowAttributeUuidArray = oldConfig.getJSONArray("showAttributeLabelList");
                    if (CollectionUtils.isNotEmpty(oldShowAttributeUuidArray)) {
                        if (CollectionUtils.isEqualCollection(oldShowAttributeUuidArray, showAttributeLabelArray)) {
                            return false;
                        }
                        JSONArray showAttributeArray = oldConfig.getJSONArray("showAttributeList");
                        if (CollectionUtils.isNotEmpty(showAttributeArray)) {
                            for (int i = 0; i < showAttributeArray.size(); i++) {
                                JSONObject showAttributeObj = showAttributeArray.getJSONObject(i);
                                if (MapUtils.isNotEmpty(showAttributeObj)) {
                                    String uuid = showAttributeObj.getString("uuid");
                                    if (uuid != null) {
                                        oldShowAttributeUuidMap.put(showAttributeObj.getString("label"), uuid);
                                        DependencyManager.delete(CiAttr2MatrixAttrDependencyHandler.class, uuid);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
//        CiViewVo searchVo = new CiViewVo();
//        searchVo.setCiId(ciId);
//        Map<String, CiViewVo> ciViewMap = new HashMap<>();
//        List<CiViewVo> ciViewList = RelUtil.ClearCiViewRepeatRel(ciViewMapper.getCiViewByCiId(searchVo));
//        for (CiViewVo ciview : ciViewList) {
//            switch (ciview.getType()) {
//                case "attr":
//                    ciViewMap.put("attr_" + ciview.getItemId(), ciview);
//                    break;
//                case "relfrom":
//                    ciViewMap.put("relfrom_" + ciview.getItemId(), ciview);
//                    break;
//                case "relto":
//                    ciViewMap.put("relto_" + ciview.getItemId(), ciview);
//                    break;
//                case "const":
//                    //固化属性需要特殊处理
//                    ciViewMap.put("const_" + ciview.getItemName().replace("_", ""), ciview);
//                    break;
//            }
//        }
        JSONArray showAttributeArray = new JSONArray();
        if (!showAttributeLabelArray.contains("const_id")) {
            showAttributeLabelArray.add(0, "const_id");
        }
        Iterator<Object> iterator = showAttributeLabelArray.iterator();
        while (iterator.hasNext()) {
            String showAttributeLabel = (String) iterator.next();
            JSONObject showAttributeObj = new JSONObject();
            String showAttributeUuid = oldShowAttributeUuidMap.get(showAttributeLabel);
            if (showAttributeUuid == null) {
                showAttributeUuid = UuidUtil.randomUuid();
            }
            showAttributeObj.put("uuid", showAttributeUuid);
//            CiViewVo ciViewVo = ciViewMap.get(showAttributeLabel);
//            if (ciViewVo == null) {
//                iterator.remove();
//                continue;
//            }
//            showAttributeObj.put("name", ciViewVo.getItemLabel());
            showAttributeObj.put("label", showAttributeLabel);
            showAttributeArray.add(showAttributeObj);
            if (showAttributeLabel.startsWith("const_")) {
                continue;
            }
//            JSONObject dependencyConfig = new JSONObject();
//            dependencyConfig.put("matrixUuid", matrixVo.getUuid());
//            dependencyConfig.put("customViewId", customViewId);
//            DependencyManager.insert(CiAttr2MatrixAttrDependencyHandler.class, showAttributeLabel.split("_")[1], showAttributeUuid, dependencyConfig);
        }
        config.put("showAttributeList", showAttributeArray);
        MatrixCmdbCustomViewVo matrixCmdbCustomViewVo = new MatrixCmdbCustomViewVo(matrixVo.getUuid(), customViewId, config);
        matrixMapper.replaceMatrixCmdbCustomView(matrixCmdbCustomViewVo);
        return true;
    }

    @Override
    protected void myGetMatrix(MatrixVo matrixVo) {
        MatrixCmdbCustomViewVo matrixCmdbCustomViewVo = matrixMapper.getMatrixCmdbCustomViewByMatrixUuid(matrixVo.getUuid());
        if (matrixCmdbCustomViewVo == null) {
            throw new MatrixCmdbCustomViewNotFoundException(matrixVo.getUuid());
        }
        matrixVo.setCustomViewId(matrixCmdbCustomViewVo.getCustomViewId());
        JSONObject config = matrixCmdbCustomViewVo.getConfig();
        CustomViewVo customView = customViewMapper.getCustomViewById(matrixCmdbCustomViewVo.getCustomViewId());
        if (customView != null) {
            config.put("customViewName", customView.getName());
//            config.put("ciLabel", customView.getLabel());
        }
        matrixVo.setConfig(config);
    }

    @Override
    protected void myDeleteMatrix(String uuid) {
        MatrixCmdbCustomViewVo matrixCmdbCustomViewVo = matrixMapper.getMatrixCmdbCustomViewByMatrixUuid(uuid);
        if (matrixCmdbCustomViewVo != null) {
            matrixMapper.deleteMatrixCmdbCustomViewByMatrixUuid(uuid);
            JSONObject config = matrixCmdbCustomViewVo.getConfig();
            if (MapUtils.isNotEmpty(config)) {
                JSONArray showAttributeArray = config.getJSONArray("showAttributeList");
                if (CollectionUtils.isNotEmpty(showAttributeArray)) {
                    for (int i = 0; i < showAttributeArray.size(); i++) {
                        JSONObject showAttributeObj = showAttributeArray.getJSONObject(i);
                        if (MapUtils.isNotEmpty(showAttributeObj)) {
                            Long id = showAttributeObj.getLong("id");
                            if (id != null) {
//                                DependencyManager.delete(CiAttr2MatrixAttrDependencyHandler.class, id);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void myCopyMatrix(String sourceUuid, MatrixVo matrixVo) {

    }

    @Override
    protected JSONObject myImportMatrix(MatrixVo matrixVo, MultipartFile multipartFile) throws IOException {
        return null;
    }

    @Override
    protected MatrixVo myExportMatrix(MatrixVo matrixVo) {
        myGetMatrix(matrixVo);
        return matrixVo;
    }

    @Override
    protected void myImportMatrix(MatrixVo matrixVo) {
        matrixMapper.deleteMatrixCmdbCustomViewByMatrixUuid(matrixVo.getUuid());
        MatrixCmdbCustomViewVo matrixCmdbCustomViewVo = new MatrixCmdbCustomViewVo(matrixVo.getUuid(), matrixVo.getCustomViewId(), matrixVo.getConfig());
        matrixMapper.replaceMatrixCmdbCustomView(matrixCmdbCustomViewVo);
    }

    @Override
    protected void mySaveAttributeList(String matrixUuid, List<MatrixAttributeVo> matrixAttributeList) {

    }

    @Override
    protected List<MatrixAttributeVo> myGetAttributeList(MatrixVo matrixVo) {
        Long customViewId = null;
        Map<String, String> showAttributeUuidMap = new HashMap<>();
        String matrixUuid = matrixVo.getUuid();
        if (StringUtils.isNotBlank(matrixUuid)) {
            MatrixCmdbCustomViewVo matrixCmdbCustomViewVo = matrixMapper.getMatrixCmdbCustomViewByMatrixUuid(matrixUuid);
            if (matrixCmdbCustomViewVo == null) {
                throw new MatrixCmdbCustomViewNotFoundException(matrixUuid);
            }
            customViewId = matrixCmdbCustomViewVo.getCustomViewId();
            JSONObject config = matrixCmdbCustomViewVo.getConfig();
            JSONArray showAttributeArray = config.getJSONArray("showAttributeList");
            for (int i = 0; i < showAttributeArray.size(); i++) {
                JSONObject showAttributeObj = showAttributeArray.getJSONObject(i);
                showAttributeUuidMap.put(showAttributeObj.getString("label"), showAttributeObj.getString("uuid"));
            }
        } else {
            customViewId = matrixVo.getCustomViewId();
        }
        CustomViewVo customView = customViewMapper.getCustomViewById(customViewId);
        if (customView == null) {
            throw new CustomViewNotFoundException(customViewId);
        }
        int sort = 0;
        List<MatrixAttributeVo> matrixAttributeList = new ArrayList<>();
//        CiViewVo ciViewVo = new CiViewVo();
//        ciViewVo.setCiId(ciId);
//        List<CiViewVo> ciViewList = RelUtil.ClearCiViewRepeatRel(ciViewMapper.getCiViewByCiId(ciViewVo));
//        if (CollectionUtils.isNotEmpty(ciViewList)) {
//            List<AttrVo> attrList = attrMapper.getAttrByCiId(ciId);
//            Map<Long, AttrVo> attrMap = attrList.stream().collect(Collectors.toMap(AttrVo::getId, e -> e));
//            List<RelVo> relList = RelUtil.ClearRepeatRel(relMapper.getRelByCiId(ciId));
//            Map<Long, RelVo> fromRelMap = relList.stream().filter(rel -> rel.getDirection().equals("from")).collect(Collectors.toMap(RelVo::getId, e -> e));
//            Map<Long, RelVo> toRelMap = relList.stream().filter(rel -> rel.getDirection().equals("to")).collect(Collectors.toMap(RelVo::getId, e -> e));
//            for (CiViewVo ciview : ciViewList) {
//                MatrixAttributeVo matrixAttributeVo = new MatrixAttributeVo();
//                matrixAttributeVo.setMatrixUuid(matrixUuid);
//                matrixAttributeVo.setName(ciview.getItemLabel());
//                matrixAttributeVo.setType(MatrixAttributeType.INPUT.getValue());
//                matrixAttributeVo.setIsDeletable(0);
//                matrixAttributeVo.setSort(sort++);
//                matrixAttributeVo.setIsRequired(0);
//                switch (ciview.getType()) {
//                    case "attr":
//                        matrixAttributeVo.setLabel("attr_" + ciview.getItemId());
//                        AttrVo attrVo = attrMap.get(ciview.getItemId());
//                        JSONObject attrConfig = new JSONObject();
//                        attrConfig.put("attr", attrVo);
//                        matrixAttributeVo.setConfig(attrConfig);
//                        break;
//                    case "relfrom":
//                        matrixAttributeVo.setLabel("relfrom_" + ciview.getItemId());
//                        RelVo fromRelVo = fromRelMap.get(ciview.getItemId());
//                        JSONObject fromRelConfig = new JSONObject();
//                        fromRelConfig.put("rel", fromRelVo);
//                        matrixAttributeVo.setConfig(fromRelConfig);
//                        break;
//                    case "relto":
//                        matrixAttributeVo.setLabel("relto_" + ciview.getItemId());
//                        RelVo toRelVo = toRelMap.get(ciview.getItemId());
//                        JSONObject toRelConfig = new JSONObject();
//                        toRelConfig.put("rel", toRelVo);
//                        matrixAttributeVo.setConfig(toRelConfig);
//                        break;
//                    case "const":
//                        //固化属性需要特殊处理
//                        String itemName = ciview.getItemName().replace("_", "");
//                        matrixAttributeVo.setLabel("const_" + itemName);
//                        if ("id".equals(itemName)) {
//                            matrixAttributeVo.setPrimaryKey(1);
//                        } else if ("ciLabel".equals(itemName)) {
//                            // 不是虚拟模型的模型属性不能搜索
//                            if (Objects.equals(ciVo.getIsAbstract(), 0)) {
//                                matrixAttributeVo.setIsSearchable(0);
//                            }
//                            JSONObject config = new JSONObject();
//                            config.put("ciId", ciId);
//                            matrixAttributeVo.setConfig(config);
//                        } else {
//                            matrixAttributeVo.setIsSearchable(0);
//                        }
//                        break;
//                    default:
//                        break;
//                }
//                if (StringUtils.isBlank(matrixAttributeVo.getLabel())) {
//                    continue;
//                }
//                if (MapUtils.isNotEmpty(showAttributeUuidMap)) {
//                    String uuid = showAttributeUuidMap.get(matrixAttributeVo.getLabel());
//                    if (uuid == null && Objects.equals(matrixAttributeVo.getPrimaryKey(), 0)) {
//                        continue;
//                    }
//                    matrixAttributeVo.setUuid(uuid);
//                }
//                matrixAttributeList.add(matrixAttributeVo);
//            }
//        }
        return matrixAttributeList;
    }

    @Override
    protected JSONObject myExportAttribute(MatrixVo matrixVo) {
        return null;
    }

    @Override
    protected JSONObject myTableDataSearch(MatrixDataVo dataVo) {
        return null;
    }

    @Override
    protected List<Map<String, JSONObject>> mySearchTableDataNew(MatrixDataVo dataVo) {
        return null;
    }

    @Override
    protected JSONObject mySaveTableRowData(String matrixUuid, JSONObject rowData) {
        return null;
    }

    @Override
    protected Map<String, String> myGetTableRowData(MatrixDataVo matrixDataVo) {
        return null;
    }

    @Override
    protected void myDeleteTableRowData(String matrixUuid, List<String> uuidList) {

    }
}
