package com.bigdata.kettle.utils;

import java.util.Map;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.ConsoleLoggingEventListener;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEventListener;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java调用Kettle工具类
 *
 * @classname KettleUtil
 * @date 2020/3/24 16:24
 */
public final class KettleUtil {

    private static final Logger log = LoggerFactory.getLogger(KettleUtil.class);

    private static final String KETTLE_BASE_DIR = "D:\\kettle";
    private static final String KETTLE_KET_DIR = KETTLE_BASE_DIR + "\\ktr";
    private static final String KETTLE_PLUGIN_DIR = KETTLE_BASE_DIR + "\\plugins";
    
    private static final KettleLoggingEventListener listener = new ConsoleLoggingEventListener();

    static {
        try {
            log.debug("加载Kettle插件");
            StepPluginType.getInstance().getPluginFolders().add(new PluginFolder(KETTLE_PLUGIN_DIR, false, true));
            log.debug("Kettle插件加载成功");
            log.debug("初始化Kettle插件环境");
            KettleEnvironment.init();
            log.debug("Kettle插件环境初始化完成");
        } catch (KettleException e) {
            log.error("Kettle插件加载初始化失败");
        }
    }

    /**
     * 调用Kettle插件执行KTR文件
     *
     * @param ktrName 目标KTR文件名称
     * @param params KTR文件所需要的参数信息
     *
     * @return void
     * @date 2020/3/24 16:31
     */
    public static void runKtr(String ktrName, Map<String, String> params) {
    	
        try {
            log.debug("开始执行Kettler任务");

            log.debug("创建TransMeta源数据对象");
            TransMeta transMeta = new TransMeta(KETTLE_KET_DIR + "\\" + ktrName);

            log.debug("传入KTR文件所需要的参数值");
            if (null != params) {
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    transMeta.setVariable(entry.getKey(), entry.getValue());
                }
            }
            Trans transformation = new Trans(transMeta);

            log.debug("设置Kettle日志级别");
            transformation.setLogLevel(LogLevel.BASIC);

            log.debug("创建日志监听程序");
            
            KettleLogStore.getAppender().addLoggingEventListener(listener);

            log.debug("开始执行Kettle文件");
            transformation.prepareExecution(null);
            transformation.startThreads();

            log.debug("等待Kettle执行完成");
            transformation.waitUntilFinished();

            if (transformation.getErrors() > 0) {
                log.error("Kettle执行过程中发生异常");
                throw new RuntimeException("Kettle执行过程中发生异常,请查看日志信息");
            } else {
                log.debug("Kettle执行成功");
            }
        } catch (Exception e) {
            log.error("Kettle执行失败", e);
        } finally {
        	KettleLogStore.getAppender().removeLoggingEventListener(listener);
            log.debug("Kettle执行结束");
        }
    }


    /**
     * 调用Kettle执行作业文件
     *
     * @methodname runJob
     * @param kjbName 作业文件名称
     * @param params 作业参数
     *
     * @return void
     * @date 2020/3/24 18:33
     */
    public static void runJob(String kjbName, Map<String, String> params) throws Exception {
        try {
            log.debug("开始执行Kettle任务");

            log.debug("创建Job的源数据对象");
            JobMeta jobMeta = new JobMeta(KETTLE_KET_DIR + "\\" + kjbName, null);
            log.debug("创建Job对象");
            Job job = new Job(null, jobMeta);

            log.debug("传入作业执行需要的参数");
            if (null != params) {
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    job.setVariable(entry.getKey(), entry.getValue());
                }
            }

            log.debug("设置Kettle日志级别");
            job.setLogLevel(LogLevel.BASIC);

            log.debug("创建日志监听程序");
            KettleLoggingEventListener kettlelog = new ConsoleLoggingEventListener();
            KettleLogStore.getAppender().addLoggingEventListener(kettlelog);

            log.debug("开始执行Job");
            job.start();

            log.debug("等待Job执行完成");
            job.waitUntilFinished();
            job.setFinished(true);

            if (job.getErrors() > 0) {
                log.error("Job执行过程中发生异常");
                throw new RuntimeException("Job执行过程中发生异常,请查看日志信息");
            } else {
                log.debug("Job执行成功");
            }
        } catch (Exception e) {
            log.error("Kettle执行失败", e);
        } finally {
        	KettleLogStore.getAppender().removeLoggingEventListener(listener);
            log.debug("Kettle执行结束");
        }
    }

}