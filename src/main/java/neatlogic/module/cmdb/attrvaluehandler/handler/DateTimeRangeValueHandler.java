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

package neatlogic.module.cmdb.attrvaluehandler.handler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.cmdb.attrvaluehandler.core.IAttrValueHandler;
import neatlogic.framework.cmdb.dto.ci.AttrVo;
import neatlogic.framework.cmdb.enums.SearchExpression;
import neatlogic.framework.cmdb.exception.validator.DatetimeAttrFormatIrregularException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.stereotype.Service;

import java.text.ParseException;


@Service
public class DateTimeRangeValueHandler implements IAttrValueHandler {

    @Override
    public String getType() {
        return "datetimerange";
    }

    @Override
    public String getName() {
        return "时间范围";
    }

    @Override
    public String getIcon() {
        return "tsfont-cluster-software";
    }

    @Override
    public boolean isCanSearch() {
        return true;
    }

    @Override
    public boolean isCanInput() {
        return true;
    }

    @Override
    public boolean isCanSort() {
        return false;
    }

    @Override
    public boolean isCanImport() {
        return true;
    }

    @Override
    public boolean isSimple() {
        return true;
    }

    @Override
    public boolean isNeedTargetCi() {
        return false;
    }

    @Override
    public boolean isNeedConfig() {
        return true;
    }

    @Override
    public boolean isNameAttr() {
        return false;
    }

    @Override
    public boolean isUniqueAttr() {
        return false;
    }

    @Override
    public boolean isNeedWholeRow() {
        return false;
    }

    @Override
    public SearchExpression[] getSupportExpression() {
        return new SearchExpression[]{SearchExpression.EQ, SearchExpression.NE, SearchExpression.NOTNULL, SearchExpression.NULL};
    }

    @Override
    public void transferValueListToSave(AttrVo attrVo, JSONArray valueList) {
        if (CollectionUtils.isNotEmpty(valueList)) {
            int len = valueList.size();
            for (int i = len - 1; i >= 0; i--) {
                String v = valueList.getString(i);
                if (StringUtils.isBlank(v)) {
                    valueList.remove(i);
                }
            }
        }
        if (CollectionUtils.isNotEmpty(valueList)) {
            JSONObject config = attrVo.getConfig();
            String type = config.getString("type");
            String format = config.getString("format");
            if (StringUtils.isNotBlank(type) && StringUtils.isNotBlank(format)) {
                for (int i = 0; i < valueList.size(); i++) {
                    try {
                        //数据库存的是yyyy-mm-dd,yyyy-mm-dd，显示是yyyy-mm-dd~yyyy-mm-dd，所以需要转换，因为excel导入时用的是显示格式
                        String v = valueList.getString(i);
                        if (v.contains("~")) {
                            v = v.replace("~", ",");
                        }
                        String[] vs = v.split(",");
                        if (vs.length != 2) {
                            throw new DatetimeAttrFormatIrregularException(attrVo, valueList.getString(i), format + "~" + format);
                        }
                        DateUtils.parseDate(vs[0], format);
                        DateUtils.parseDate(vs[1], format);
                        valueList.set(i, v);
                    } catch (ParseException e) {
                        throw new DatetimeAttrFormatIrregularException(attrVo, valueList.getString(i), format + "~" + format);
                    }
                }
            }
        }
    }

    @Override
    public JSONArray getActualValueList(AttrVo attrVo, JSONArray valueList) {
        JSONArray returnList = new JSONArray();
        for (int i = 0; i < valueList.size(); i++) {
            try {
                String v = valueList.getString(i);
                if (v.contains(",")) {
                    v = v.replace(",", "~");
                }
                returnList.add(v);
            } catch (Exception ignored) {

            }
        }
        return returnList;
    }

    /**
     * 将值转换成显示的形式
     *
     * @param valueList 数据库的数据
     * @return 用于显示数据
     */
    @Override
    public void transferValueListToDisplay(AttrVo attrVo, JSONArray valueList) {
        for (int i = 0; i < valueList.size(); i++) {
            try {
                String v = valueList.getString(i);
                if (v.contains(",")) {
                    v = v.replace(",", "~");
                }
                valueList.set(i, v);
            } catch (Exception ignored) {

            }
        }
    }


    @Override
    public int getSort() {
        return 10;
    }

}
