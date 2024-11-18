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

package neatlogic.module.cmdb.mq.subscribe;

import neatlogic.framework.mq.core.SubscribeHandlerBase;
import neatlogic.framework.mq.dto.SubscribeVo;
import org.springframework.stereotype.Service;

@Service
public class CiEntityInsertSubscribe extends SubscribeHandlerBase {
    @Override
    public String getName() {
        return "配置添加处理组件";
    }


    @Override
    protected void myOnMessage(SubscribeVo subscribeVo, Object message) {
        System.out.println("from " + subscribeVo.getHandler() + ":" + message);
    }
}
