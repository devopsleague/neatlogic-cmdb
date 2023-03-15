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

package neatlogic.module.cmdb.api.ci;

import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.cmdb.dto.ci.CiVo;
import neatlogic.framework.cmdb.exception.ci.CiAuthException;
import neatlogic.framework.cmdb.exception.ci.VirtualCiSettingFileNotFoundException;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.common.util.FileUtil;
import neatlogic.framework.file.dao.mapper.FileMapper;
import neatlogic.framework.file.dto.FileVo;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.framework.util.RegexUtils;
import neatlogic.framework.cmdb.auth.label.CI_MODIFY;
import neatlogic.module.cmdb.service.ci.CiAuthChecker;
import neatlogic.module.cmdb.service.ci.CiService;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Service
@AuthAction(action = CI_MODIFY.class)
@OperationType(type = OperationTypeEnum.CREATE)
public class SaveCiApi extends PrivateApiComponentBase {

    @Resource
    private CiService ciService;

    @Resource
    private FileMapper fileMapper;


    @Override
    public String getToken() {
        return "/cmdb/ci/save";
    }

    @Override
    public String getName() {
        return "保存模型";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({@Param(name = "id", type = ApiParamType.LONG, desc = "id，不提供代表新增模型"),
            @Param(name = "name", type = ApiParamType.REGEX, rule = RegexUtils.ENGLISH_NUMBER_NAME, xss = true, isRequired = true, maxLength = 25, desc = "唯一标识"),
            @Param(name = "label", type = ApiParamType.STRING, desc = "中文名称", xss = true, maxLength = 100,
                    isRequired = true),
            @Param(name = "description", type = ApiParamType.STRING, desc = "备注", maxLength = 500, xss = true),
            @Param(name = "icon", type = ApiParamType.STRING, isRequired = true, desc = "图标"),
            @Param(name = "typeId", type = ApiParamType.LONG, desc = "类型id", isRequired = true),
            @Param(name = "parentCiId", type = ApiParamType.LONG, desc = "父配置项id"),
            @Param(name = "fileId", type = ApiParamType.LONG, desc = "虚拟模型配置文件id"),
            @Param(name = "expiredDay", type = ApiParamType.INTEGER, desc = "超时天数"),
            @Param(name = "isAbstract", type = ApiParamType.INTEGER, defaultValue = "0", desc = "是否抽象模型"),
            @Param(name = "isVirtual", type = ApiParamType.INTEGER, defaultValue = "0", desc = "是否虚拟模型"),
            @Param(name = "isPrivate", type = ApiParamType.INTEGER, defaultValue = "0", desc = "是否私有模型"),
            @Param(name = "isMenu", type = ApiParamType.INTEGER, defaultValue = "0", desc = "是否在菜单显示")})
    @Output({@Param(name = "id", type = ApiParamType.STRING, desc = "模型id")})
    @Description(desc = "保存模型接口")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        CiVo ciVo = JSONObject.toJavaObject(jsonObj, CiVo.class);
        Long ciId = jsonObj.getLong("id");
        if (Objects.equals(ciVo.getIsVirtual(), 1) && ciVo.getFileId() != null) {
            FileVo fileVo = fileMapper.getFileById(ciVo.getFileId());
            if (fileVo == null) {
                throw new VirtualCiSettingFileNotFoundException();
            }
            String xml = IOUtils.toString(FileUtil.getData(fileVo.getPath()), StandardCharsets.UTF_8);
            if (StringUtils.isBlank(xml)) {
                throw new VirtualCiSettingFileNotFoundException();
            }
            ciVo.setViewXml(xml);
        }
        if (ciId == null) {
            if (!CiAuthChecker.chain().checkCiManagePrivilege().check()) {
                throw new CiAuthException();
            }
            ciService.insertCi(ciVo);
        } else {
            if (!CiAuthChecker.chain().checkCiManagePrivilege(ciId).check()) {
                throw new CiAuthException();
            }
            ciService.updateCi(ciVo);
        }
        return ciVo.getId();
    }

}