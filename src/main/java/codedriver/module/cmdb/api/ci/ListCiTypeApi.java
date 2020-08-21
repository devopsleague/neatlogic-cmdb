package codedriver.module.cmdb.api.ci;

import java.util.Comparator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.reminder.core.OperationTypeEnum;
import codedriver.framework.restful.annotation.Description;
import codedriver.framework.restful.annotation.Input;
import codedriver.framework.restful.annotation.OperationType;
import codedriver.framework.restful.annotation.Output;
import codedriver.framework.restful.annotation.Param;
import codedriver.framework.restful.core.ApiComponentBase;
import codedriver.module.cmdb.dao.mapper.ci.CiTypeMapper;
import codedriver.module.cmdb.dto.ci.CiTypeVo;

@Service
@OperationType(type = OperationTypeEnum.SEARCH)
public class ListCiTypeApi extends ApiComponentBase {

    @Autowired
    private CiTypeMapper ciTypeMapper;

    @Override
    public String getToken() {
        return "/cmdb/citype/list";
    }

    @Override
    public String getName() {
        return "获取模型类型列表";
    }

    @Override
    public String getConfig() {
        // TODO Auto-generated method stub
        return null;
    }

    @Output({@Param(explode = CiTypeVo[].class)})
    @Description(desc = "获取模型类型列表接口")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        return ciTypeMapper.searchCiType(new CiTypeVo());
    }
}
