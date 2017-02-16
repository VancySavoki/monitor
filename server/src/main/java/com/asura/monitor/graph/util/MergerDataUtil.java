package com.asura.monitor.graph.util;

import com.asura.util.DateUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;

import static com.asura.monitor.graph.util.FileRender.readHistory;
import static com.asura.monitor.graph.util.FileWriter.dataDir;
import static com.asura.monitor.graph.util.FileWriter.separator;
import static com.asura.util.DateUtil.getLastNDay;
import static com.asura.util.DateUtil.getYestDay;

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
 * 每天做一次数据存储
 */
public class MergerDataUtil extends Thread {

    private Logger logger = Logger.getLogger(MergerDataUtil.class);

    private String groups;
    private String name;
    private String ip;

    // 每个文件最大存储条数
    private int maxNumber = 1200;
    // 天数
    private int dayNumber;

    private DecimalFormat df = new DecimalFormat("######0.00");

    private String fileName;

    public MergerDataUtil(String groups, String name, int dayNumber, String ip) {
        this.groups = groups;
        this.name = name;
        this.ip = ip;
        this.dayNumber = dayNumber;
    }

    public void run(){
        init();
        mergerData(dayNumber);
    }

    void init(){
        setFileName(dayNumber);
    }

    /**
     * 合并数据
     * @param day
     */
    void mergerData(int day){
        File file = new File(fileName);
        if (file.exists()){
            // 获取一天需要的数据量
            int interval = maxNumber / day ;
            // 获取一天的时间的数据
            ArrayList<ArrayList> datas = getDatas(day);
            // 获取一天数据需要合并的条目数
            int mergerNumber = datas.size() / interval;
            StringBuffer writeData = getWriteData(datas,  mergerNumber);
            FileWriter.writeFile(fileName, writeData.toString(), true);
            int rows = FileRender.getFileRows(fileName);
            if (rows >= maxNumber) {
//                logger.info("删除 " + fileName + (rows - maxNumber) + "  行");
                FileWriter.deleteFileLine(fileName, rows - maxNumber);
            }
        }else{
            logger.info("开始初始化合并文件: "+fileName);
            mergerDayData(day);
        }
    }

    /**
     * 获取指定天数的数据
     * @return
     */
    ArrayList<ArrayList> getDatas(int dayNumber){
        String startT = getLastNDay(dayNumber);
        String endT = getYestDay();
        ArrayList<ArrayList> datas;
        datas = readHistory(ip, groups, name, startT, endT, null);
        return datas;
    }

    /**
     * 检查文件是否正常
     * @return
     */
    boolean checkFileTime(String lastData, String date){
        try {
            if (lastData != null && lastData.length() > 3) {
                String[] datas = lastData.split(" ");
                long lastDate = Long.valueOf(datas[0]);
                if (lastDate < Long.valueOf(date)) {
                    return true;
                }
            } else {
                return true;
            }
        }catch (Exception e){
            return true;
        }
        return false;
    }

    /**
     *
     * @param datas
     * @param interval
     * @return
     */
    StringBuffer getWriteData(ArrayList<ArrayList> datas, int interval){
        String lastData = FileRender.readLastLine(fileName);
        StringBuffer writeData = new StringBuffer();
        int counter = 0;
        Double value = 1.0;
        for (ArrayList<Double> data : datas) {
            if(!checkFileTime(lastData, ""+data.get(0))){
                continue;
            }
            if (counter % interval == 0) {
                value = value / interval;
                writeData.append(data.get(0) + " " + df.format(value) + "\n");
                value = 1.0;
            } else {
                value += data.get(1);
            }
            counter += 1;
            if (counter >= datas.size()){
                writeData.append(data.get(0) + " " + df.format(value) + "\n");
            }
        }
        return writeData;
    }

    /**
     * 按天的数据直接合并重写
     */
    void mergerDayData(int dayNumber) {
        ArrayList<ArrayList> datas = getDatas(dayNumber);
        StringBuffer writeData = getWriteData(datas, dayNumber);
        FileWriter.writeFile(fileName, writeData.toString(), false);
    }

// 获取目录
    void setFileName(int dayNumber) {
        DateUtil dateUtil = new DateUtil();
        String dir = dataDir
                + separator + "graph"
                + separator + ip
                + separator + groups
                + separator + dateUtil.getDate("yyyy")
                + separator + "day" + dayNumber
                + separator;
        this.fileName = dir + name;
    }
}
