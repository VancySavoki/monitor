package com.asura.monitor.graph.util;

import com.asura.util.DateUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

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
 * 数据合并工具
 *
 * @author zhaozq
 * @version 1.0
 * @Date 2017-02-04
 */

public class DataMergerUtil extends Thread {

    private Logger logger = Logger.getLogger(DataMergerUtil.class);

    private String groups;
    private String name;
    private String ip;
    // 每天数量的基数
    private int baseNumber = 1;
    // 天数
    private int dayNumber = 1;
    // 最多显示数据
    private int maxNumber = 700;
    // 合并条数
    private int mergerNumber;
    // 文件名
    private String fileName;
    private DecimalFormat df = new DecimalFormat("######0.00");

    public DataMergerUtil(String groups, String name, int dayNumber, String ip) {
        this.groups = groups;
        this.name = name;
        this.ip = ip;
        this.dayNumber = dayNumber;
    }

    public void run() {
        init();
        // 月数据
        int rows = FileRender.getFileRows(fileName);
        if(rows < maxNumber ) {
            mergerStart();
        }else {
            getGt30DayData();
        }
    }

    void init() {
        createDataFile();
        setBaseNumber();
        setMergerNumber();
    }

    void mergerLastYear() {
        String[] lastDate = DateUtil.getLastNDay(3).split("-");
        String year = lastDate[0];
        String dir = dataDir + separator + "graph" + separator + ip + separator + groups + separator + year + separator + "day" + dayNumber + separator;
        FileWriter.writeFile(fileName, FileRender.readFile(dir + name), false);
    }

    void mergerStart() {
        int rows = FileRender.getFileRows(fileName);
        // 如果数据不足，就写入数据
        if (rows <= maxNumber) {
            dataMerger();
            return;
        }

        // 如果时间是01月01日志，那么数据就把去年的复制过来
        if (DateUtil.getDate(DateUtil.DATE_FORMAT).contains("01-01")) {
            mergerLastYear();
            return;
        }

        Double value = 1.0;
        // 当数据大于最大数量后，开始实现追加和删除第一行，环形写入数据
        if (rows >= maxNumber) {
            // 获取每天数据量
            ArrayList<ArrayList> datas = readHistory(ip, groups, name, getYestDay(), getYestDay(), null);
            for (ArrayList<Double> d : datas) {
                    value += Double.valueOf(d.get(1));
            }
            value = value / datas.size();
            String writeData = DateUtil.getDateStampInteter() + "000 " + df.format(value);
            FileWriter.writeFile(fileName, writeData, true);
            // 每次写入一行，删除一行
            FileWriter.deleteFileLine(fileName, 1);
        }
    }

    // 合并数据
    void merger(ArrayList<ArrayList> datas) {
        int counter = 0;
        Double value = 1.0;
        String writeData = "";
        this.mergerNumber = datas.size() / maxNumber ;
        for (ArrayList<Double> data : datas) {
            // 当合并条数为1时，不做合并
            if (mergerNumber == 1) {
                value = data.get(1);
                writeData += data.get(0) + " " + df.format(value) + "\n";
            } else {
                value += data.get(1);
            }
            // 达到合并的条目后执行一次写入
            if (counter % mergerNumber == 0 && mergerNumber > 1) {
                // 几条数据的平均数
                value = value / mergerNumber;
                writeData += data.get(0) + " " + df.format(value) + "\n";
                value = 1.0;
            }
            counter += 1;
            if (writeData.length() > 1024 * 20 || counter >= datas.size()) {
                FileWriter.writeFile(fileName, writeData, true);
                writeData = "";
            }
        }
    }

    /**
     * 处理大于30天的数据
     */
    void getGt30DayData() {
        String day30 = getDir(30) + name;
        ArrayList<ArrayList> datas = new ArrayList<>();
        // 当数据大于30天时，从30天威基础数据
        datas = FileRender.readTxtFile(day30, datas, null);
        // 每天更新一次数据
        // 如果文件为空，那么僵30天的数据全部写入
        int rows = FileRender.getFileRows(fileName);
        String writeData = "";

        if (rows < 31) {
            for (ArrayList<Double> data : datas) {
                writeData += data.get(0) + " " + df.format(data.get(1)) + "\n";
            }
            FileWriter.writeFile(fileName, writeData, false);
            return;
        }

        // 大于一个月的数据后，开始追加文件，直到到达最大数量
        if (rows > 31 && rows < maxNumber) {
            writeData = FileRender.readLastLine(day30);
            FileWriter.writeFile(fileName, writeData, true);
            return;
        }

        // 如果时间是01月01日志，那么数据就把去年的复制过来
        if (DateUtil.getDate(DateUtil.DATE_FORMAT).contains("01-01")) {
            mergerLastYear();
            return;
        }

        String[] tempData;
        Double value = 1.0;
        // 当数据大于最大数量后，开始实现追加和删除第一行，环形写入数据
        // 按  ( 120 / 30 ) = 4 每4天更新一次数据, 读取day30的最后4行,然后平均
        if (rows >= maxNumber) {
            int interval = dayNumber / 30;
            List<String> data = FileRender.readLastNLine(day30, interval);
            for (String d : data) {
                tempData = d.split(" ");
                if (tempData.length > 1) {
                    value += Double.valueOf(tempData[1]);
                }
            }
            value = value / interval;
            writeData = DateUtil.getDateStampInteter() + "000 " + df.format(value);
            FileWriter.writeFile(fileName, writeData, true);
            // 每次写入一行，删除一行
            FileWriter.deleteFileLine(fileName, 1);
        }
    }

    // 输入读取和合并
    void dataMerger() {
        String startT = getLastNDay(dayNumber);
        String endT = getYestDay();
        ArrayList<ArrayList> datas;
        datas = readHistory(ip, groups, name, startT, endT, null);
        merger(datas);
    }

    // 获取数据读取的时间段
    ArrayList<String> getDateList() {
        String startT = getLastNDay(dayNumber);
        String endT = getYestDay();
        return FileRender.findDates(startT, endT);
    }

    // 获取合并的条数基准数
    //1745 / (30000 / ((52350 - 30000)/ 1745 + 30 ))
    void setMergerNumber() {
        try {
            this.mergerNumber = baseNumber / (maxNumber / ((baseNumber * dayNumber - maxNumber) / baseNumber + dayNumber));
            if (this.mergerNumber == 0) {
                this.mergerNumber = 1;
            }
        } catch (Exception e) {
            this.mergerNumber = 1;
        }
    }

    // 从最近4天的数据的文件名获取数据基数
    void setBaseNumber() {
        String startT = getLastNDay(5);
        String endT = DateUtil.getYestDay();
        ArrayList arrayList = FileRender.readHistory(ip, groups, name, startT, endT, null);
        this.baseNumber = arrayList.size() / 4;
    }

    //  创建需要的目录和文件
    void createDataFile() {
        String dir = getDir(dayNumber);
        if (!new File(dir).exists()) {
            FileWriter.makeDir(dir);
        }
        String file = dir + name;
        this.fileName = file;
        if (!new File(file).exists()) {
            FileWriter.writeFile(file, "", false);
        }
    }

    // 获取目录
    String getDir(int dayNumber) {
        DateUtil dateUtil = new DateUtil();
        String dir = dataDir + separator + "graph" + separator + ip + separator + groups + separator + dateUtil.getDate("yyyy") + separator + "day" + dayNumber + separator;
        return dir;
    }

}