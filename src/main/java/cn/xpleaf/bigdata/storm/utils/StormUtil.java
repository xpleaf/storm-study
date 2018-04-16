package cn.xpleaf.bigdata.storm.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class StormUtil {

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static DateFormat df_yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");
}
