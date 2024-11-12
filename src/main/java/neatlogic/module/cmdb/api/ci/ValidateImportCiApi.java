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

package neatlogic.module.cmdb.api.ci;

import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.cmdb.auth.label.CI_MODIFY;
import neatlogic.framework.cmdb.dto.ci.CiVo;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.exception.type.ParamNotExistsException;
import neatlogic.framework.restful.annotation.Description;
import neatlogic.framework.restful.annotation.Input;
import neatlogic.framework.restful.annotation.OperationType;
import neatlogic.framework.restful.annotation.Param;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateBinaryStreamApiComponentBase;
import neatlogic.module.cmdb.dao.mapper.ci.AttrMapper;
import neatlogic.module.cmdb.dao.mapper.ci.RelMapper;
import neatlogic.module.cmdb.service.ci.CiService;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipInputStream;

@Service
@AuthAction(action = CI_MODIFY.class)
@OperationType(type = OperationTypeEnum.OPERATE)
@Transactional
public class ValidateImportCiApi extends PrivateBinaryStreamApiComponentBase {
    @Resource
    private RelMapper relMapper;

    @Resource
    private CiService ciService;

    @Resource
    private AttrMapper attrMapper;


    @Override
    public String getToken() {
        return "/cmdb/ci/importvalidate";
    }

    @Override
    public String getName() {
        return "校验导入的模型文件";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "fileList", type = ApiParamType.FILE, isRequired = true, desc = "common.file")})
    @Description(desc = "校验导入的模型文件")
    @Override
    public Object myDoService(JSONObject paramObj, HttpServletRequest request, HttpServletResponse response) throws Exception {
        MultipartHttpServletRequest multipartRequest = (MultipartHttpServletRequest) request;
        List<MultipartFile> multipartFileList = multipartRequest.getFiles("fileList");
        List<CiVo> newCiList = new ArrayList<>();
        Map<Long, CiVo> newCiMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(multipartFileList)) {
            for (MultipartFile multipartFile : multipartFileList) {
                ZipInputStream zin = new ZipInputStream(multipartFile.getInputStream());
                while (zin.getNextEntry() != null) {
                    ObjectInputStream objectInputStream = new ObjectInputStream(zin);
                    CiVo ciVo = (CiVo) objectInputStream.readObject();
                    if (!newCiMap.containsKey(ciVo.getId())) {
                        newCiList.add(ciVo);
                        newCiMap.put(ciVo.getId(), ciVo);
                    }
                    zin.closeEntry();
                }
                zin.close();
            }
            return ciService.validateImportCi(newCiList);
        } else {
            throw new ParamNotExistsException("fileList");
        }
    }

}
