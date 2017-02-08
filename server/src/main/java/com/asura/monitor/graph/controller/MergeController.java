package com.asura.monitor.graph.controller;

import com.asura.framework.base.paging.SearchMap;
import com.asura.monitor.graph.thread.MergerThread;
import com.asura.resource.entity.CmdbResourceServerEntity;
import com.asura.resource.service.CmdbResourceServerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * <p></p>
 *
 * <PRE>
 * <BR>	修改记录
 * <BR>-----------------------------------------------
 * <BR>	修改日期			修改人			修改内容
 * </PRE>
 * 20170204
 *
 * @author zhaozq
 * @version 1.0
 */

@Controller
@RequestMapping("/monitor/api/data/")
public class MergeController {

    @Autowired
    private CmdbResourceServerService serverService;

    /**
     * 数据合并接口
     *
     * @return
     */
    @RequestMapping("merger")
    @ResponseBody
    public String merger(String ip) {
        SearchMap searchMap = new SearchMap();
        if (ip != null && ip.length() > 6) {
            searchMap.put("ipAddress", ip);
        }
        List<CmdbResourceServerEntity> ips = serverService.getDataList(searchMap, "selectAllIp");
        MergerThread mergerThread = new MergerThread(ips);
        mergerThread.start();
        return "ok";
    }
}
