package com.asura.monitor.graph.thread;

import com.asura.monitor.graph.util.DataMergerUtil;
import com.asura.monitor.graph.util.FileRender;
import com.asura.resource.entity.CmdbResourceServerEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.asura.monitor.graph.util.FileRender.getSubDir;

/**
 * <p></p>
 *
 * <PRE>
 * <BR>	修改记录
 * <BR>-----------------------------------------------
 * <BR>	修改日期			修改人			修改内容
 * </PRE>
 *
 * @author zhaozq
 * @version 1.0
 * @since 1.0
 */

public class MergerThread extends Thread {

    private List<CmdbResourceServerEntity> ips;
    private int dayNumber;

    public MergerThread(List<CmdbResourceServerEntity> ips, int dayNumber) {
        this.ips = ips;
        this.dayNumber = dayNumber;
    }

    public void run() {
        String ip;
        String groups;
        String name;
        ArrayList<Integer> arrayList = new ArrayList();
        arrayList.add(3);
        arrayList.add(7);
        arrayList.add(15);
        arrayList.add(60);
        arrayList.add(90);
        arrayList.add(120);
        arrayList.add(180);
        arrayList.add(240);
        arrayList.add(360);
        for (CmdbResourceServerEntity entity : this.ips) {
            ip = entity.getIpAddress();
            long start = System.currentTimeMillis() / 1000;
            ArrayList dir = getSubDir(ip);
            // 获取所有的类型
            Map<String, ArrayList> map = FileRender.getGraphName(dir, ip);
            ExecutorService executor = Executors.newFixedThreadPool(200);
            for (Map.Entry<String, ArrayList> entity1 : map.entrySet()) {
                groups = entity1.getKey();
                ArrayList names = entity1.getValue();
                    for (int i = 0; i < names.size(); i++) {
                        name = (String) names.get(i);
                        DataMergerUtil dataMergerUtil = new DataMergerUtil(groups, name, 30, ip);
                        executor.execute(dataMergerUtil);
                    }
            }
            executor.shutdown();
            try {
                while (!executor.isTerminated()) {
                    Thread.sleep(100);
                }
            } catch (Exception e) {
            }
            ExecutorService executors = Executors.newFixedThreadPool(200);
            for (Map.Entry<String, ArrayList> entity1 : map.entrySet()) {
                groups = entity1.getKey();
                ArrayList names = entity1.getValue();
                for (Integer day : arrayList) {
                    System.out.println(day);
                    for (int i = 0; i < names.size(); i++) {
                        name = (String) names.get(i);
                        DataMergerUtil dataMergerUtil = new DataMergerUtil(groups, name, day, ip);
                        executors.execute(dataMergerUtil);
                    }
                }
            }
            executors.shutdown();
            try {
                while (!executors.isTerminated()) {
                    Thread.sleep(100);
                }
            } catch (Exception e) {
            }
            System.out.println(ip + " end  " + (System.currentTimeMillis() / 1000 - start));
        }
    }
}
