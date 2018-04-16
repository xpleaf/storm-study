package cn.xpleaf.bigdata.storm.utils;

import java.io.IOException;
import java.util.Properties;

/**
 * 配置文件的工具类（单例模式）
 */
public class ConfigurationManager {

    private ConfigurationManager(){}
    private static Properties properties;
    static {
        properties = new Properties();
        try {
            properties.load(ConfigurationManager.class.getClassLoader().getResourceAsStream("resources.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
