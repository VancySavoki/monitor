package com.asura.monitor.configure.thread;

import com.asura.monitor.configure.controller.CacheController;

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

public class MakeCacheThread extends Thread {

    private CacheController cacheController;

    public MakeCacheThread(CacheController cacheController) {
        this.cacheController = cacheController;
    }

    @Override
    public void run() {
        cacheController.allCache();
    }
}
