package com.asura.monitor.graph.quartz;

import com.asura.monitor.configure.conf.MonitorCacheConfig;
import com.asura.monitor.graph.thread.MergerThread;
import com.asura.resource.entity.CmdbResourceServerEntity;
import com.asura.util.RedisUtil;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p></p>
 *
 * <PRE>
 * <BR>	修改记录
 * <BR>-----------------------------------------------
 * <BR>	修改日期			修改人			修改内容
 * </PRE>
 * 通过队列方式执行数据合并
 * @author zhaozq
 * @version 1.0
 * @since 1.0
 */

public class MergerDataQuartz {
    private final static Logger LOGGER = org.slf4j.LoggerFactory.getLogger(MergerDataQuartz.class);

    public void start() {

        ArrayList<Integer> arrayList = new ArrayList();
        arrayList.add(3);
        arrayList.add(7);
        arrayList.add(15);
        arrayList.add(30);
        arrayList.add(60);
        arrayList.add(90);
        arrayList.add(120);
        arrayList.add(180);
        arrayList.add(240);
        arrayList.add(360);
        List<CmdbResourceServerEntity> ips;
        CmdbResourceServerEntity cmdbResourceServerEntity ;
        String ip ;
        RedisUtil redisUtil = new RedisUtil();
        long size = redisUtil.llen(MonitorCacheConfig.mergerDataQueue);
        if (size > 0 ) {
            for (int qid =0 ; qid<1000; qid++) {
                ip = redisUtil.rpop(MonitorCacheConfig.mergerDataQueue);
                if (ip != null && ip.length() > 5) {
                    ips = new ArrayList<>();
                    cmdbResourceServerEntity = new CmdbResourceServerEntity();
                    cmdbResourceServerEntity.setIpAddress(ip);
                    ips.add(cmdbResourceServerEntity);
                    long start = System.currentTimeMillis()/1000;
                    LOGGER.info("开始启动任务计划 " +ip + " start "  + start);
                    ExecutorService executors = Executors.newFixedThreadPool(1);
                    MergerThread mergerThread = new MergerThread(ips, arrayList);
                    executors.execute(mergerThread);
                    executors.shutdown();
                    try {
                        while (!executors.isTerminated()) {
                            Thread.sleep(1000);
                        }
                    } catch (Exception e) {
                    }
                    LOGGER.info("开始启动任务计划 " +ip + " end "  + (System.currentTimeMillis()/1000-start));
                }
            }
        }
    }
}
