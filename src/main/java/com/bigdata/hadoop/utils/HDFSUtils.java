package com.bigdata.hadoop.utils;


import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.multipart.MultipartFile;

import com.alibaba.fastjson.JSONObject;

 
/**
 * @ClassName HDFSUtils
 * @Date 2020/8/26 15:25
 */

public class HDFSUtils {
 
    private static String hdfsPath;
    private static String hdfsName;
    private static final int bufferSize = 1024 * 1024 * 64;
    
    private static FileSystem fileSystem;
    

    private static String hdfsDirec =HDFSPropertiesUtils.getProperty("hdfs.base.directory");
    
    private static String hdfsLocalDataDirec=HDFSPropertiesUtils.getProperty("hdfs.local.data.directory");
    
 
    static {
        //设置成自己的
        hdfsPath= HDFSPropertiesUtils.getPath();
        hdfsName=HDFSPropertiesUtils.getUserName();
    }
 
    /**
     * 获取HDFS配置信息
     * @return
     */
    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsPath);
        configuration.set("HADOOP_USER_NAME",hdfsName);
        return configuration;
    }
 
    /**
     * 获取HDFS文件系统对象
     * @return
     * @throws Exception
     */
    public static FileSystem getFileSystem() throws Exception {
        /*
        //通过这种方式设置java客户端访问hdfs的身份：会以 ypp 的身份访问 hdfs文件系统目录下的路径：/user/ypp 的目录
        System.setProperty("HADOOP_USER_NAME","ypp");
        Configuration configuration = new Configuration();
        configuration.set("fs.defauleFS","hdfs://ypp:9090");
        FileSystem fileSystem = FileSystem.get(configuration);
         */
 
        /*
        客户端去操作hdfs时是有一个用户身份的，默认情况下hdfs客户端api会从jvm中获取一个参数作为自己的用户身份
        也可以在构造客户端fs对象时，通过参数传递进去
        FileSystem fileSystem = FileSystem.get(new URI(hdfsPath), getConfiguration(), hdfsName);
        */
        FileSystem fileSystem = FileSystem.get(new URI(hdfsPath), getConfiguration());
        return fileSystem;
    }
 
    /**
     * 在HDFS创建文件夹
     * @param path
     * @return
     * @throws Exception
     */
    public static boolean mkdir(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        if (existFile(path)) {
            return true;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        boolean isOk = fs.mkdirs(srcPath);
        fs.close();
        return isOk;
    }
 
    /**
     * 判断HDFS文件是否存在
     * @param path
     * @return
     * @throws Exception
     */
    public static boolean existFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        FileSystem fs = getFileSystem();
//        fs.setWorkingDirectory(new Path("/user/root/directory"));
        Path srcPath = new Path(path);
        boolean isExists = fs.exists(srcPath);
        return isExists;
    }
 
    /**
     * 读取HDFS目录信息
     * @param path
     * @return
     * @throws Exception
     */
    public static List<Map<String, Object>> readPathInfo(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path newPath = new Path(path);
        FileStatus[] statusList = fs.listStatus(newPath);
        List<Map<String, Object>> list = new ArrayList<>();
        if (null != statusList && statusList.length > 0) {
            for (FileStatus fileStatus : statusList) {
                Map<String, Object> map = new HashMap<>();
                map.put("filePath", fileStatus.getPath());
                map.put("fileStatus", fileStatus.toString());
                list.add(map);
            }
            return list;
        } else {
            return null;
        }
    }
 
    /**
     * HDFS创建文件
     * @param path
     * @param file
     * @throws Exception
     */
    public static void createFile(MultipartFile file) throws Exception {
    	
        if (StringUtils.isEmpty(hdfsDirec) || null == file.getBytes()) {
            return;
        }
        String fileName = file.getOriginalFilename();
        FileSystem fs = getFileSystem();
        // 上传时默认当前目录，后面自动拼接文件的目录
        Path newPath = new Path(hdfsDirec + File.separator + fileName);
        // 打开一个输出流
        FSDataOutputStream outputStream = fs.create(newPath);
        outputStream.write(file.getBytes());
        outputStream.close();
        fs.close();
    }
    
    /**
     * HDFS创建文件
     * @param path
     * @param file
     * @throws Exception
     */
    public static void createFile(String fileName) throws Exception {
    	
    	File file = new File(hdfsLocalDataDirec+File.separator+fileName);
    	createFile((MultipartFile)file);
    }
 
    /**
     * 读取HDFS文件内容
     * @param path
     * @return
     * @throws Exception
     */
    public static String saveFileToLocal(String path,String outPath) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        FSDataInputStream inputStream = null;
        FileOutputStream out = null;
        try {
        	out = new FileOutputStream(outPath);
            inputStream = fs.open(srcPath);
            // 防止中文乱码
            IOUtils.copyLarge(inputStream, out);

            return "success";
        } finally {
            inputStream.close();
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(out);
            fs.close();
            
        }
    }
    
    public static String readFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(srcPath);
            // 防止中文乱码
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String lineTxt = "";
            StringBuffer sb = new StringBuffer();
            while ((lineTxt = reader.readLine()) != null) {
                sb.append(lineTxt);
            }
            return sb.toString();
        } finally {
            inputStream.close();
            IOUtils.closeQuietly(inputStream);
            fs.close();
            
        }
    }
 
    /**
     * 读取HDFS文件列表
     * @param path
     * @return
     * @throws Exception
     */
    public static List<Map<String, String>> listFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
 
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        // 递归找到所有文件
        RemoteIterator<LocatedFileStatus> filesList = fs.listFiles(srcPath, true);
        List<Map<String, String>> returnList = new ArrayList<>();
        while (filesList.hasNext()) {
            LocatedFileStatus next = filesList.next();
            String fileName = next.getPath().getName();
            Path filePath = next.getPath();
            Map<String, String> map = new HashMap<>();
            map.put("fileName", fileName);
            map.put("filePath", filePath.toString());
            returnList.add(map);
        }
        fs.close();
        return returnList;
    }
 
    /**
     * HDFS重命名文件
     * @param oldName
     * @param newName
     * @return
     * @throws Exception
     */
    public static boolean renameFile(String oldName, String newName) throws Exception {
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {
            return false;
        }
        FileSystem fs = getFileSystem();
        // 原文件目标路径
        Path oldPath = new Path(oldName);
        // 重命名目标路径
        Path newPath = new Path(newName);
        boolean isOk = fs.rename(oldPath, newPath);
        fs.close();
        return isOk;
    }
 
    /**
     * 删除HDFS文件
     * @param path
     * @return
     * @throws Exception
     */
    public static boolean deleteFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        if (!existFile(path)) {
            return false;
        }
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        boolean isOk = fs.deleteOnExit(srcPath);
        fs.close();
        return isOk;
    }
 
    /**
     * 上传HDFS文件
     * @param path
     * @param uploadPath
     * @throws Exception
     */
    public static void uploadFile(String path, String uploadPath) throws Exception {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(uploadPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 上传路径
        Path clientPath = new Path(path);
        // 目标路径
        Path serverPath = new Path(uploadPath);
 
        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyFromLocalFile(false, clientPath, serverPath);
        fs.close();
    }
    
    public static void uploadFile(String fileName) throws Exception {
    	
    	String localPath= hdfsLocalDataDirec+fileName+".txt";
    	String uploadPath= hdfsDirec+fileName+".txt";
        if (StringUtils.isEmpty(localPath) || StringUtils.isEmpty(uploadPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 上传路径
        Path clientPath = new Path(localPath);
        System.out.println("HomeDirectory:"+hdfsPath);
        // 目标路径
        Path serverPath = new Path(hdfsPath+uploadPath);
 
        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyFromLocalFile(false, clientPath, serverPath);
//        fs.close();
    }
    
 
    /**
     * 下载HDFS文件
     * @param path
     * @param downloadPath
     * @throws Exception
     */
    public static void downloadFile(String path, String downloadPath) throws Exception {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(downloadPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 上传路径
        Path clientPath = new Path(path);
        // 目标路径
        Path serverPath = new Path(downloadPath);
 
        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyToLocalFile(false, clientPath, serverPath);
        fs.close();
    }
 
    /**
     * HDFS文件复制
     * @param sourcePath
     * @param targetPath
     * @throws Exception
     */
	/*
	 * public static void copyFile(String sourcePath, String targetPath) throws
	 * Exception { if (StringUtils.isEmpty(sourcePath) ||
	 * StringUtils.isEmpty(targetPath)) { return; } FileSystem fs = getFileSystem();
	 * // 原始文件路径 Path oldPath = new Path(sourcePath); // 目标路径 Path newPath = new
	 * Path(targetPath);
	 * 
	 * FSDataInputStream inputStream = null; FSDataOutputStream outputStream = null;
	 * try { inputStream = fs.open(oldPath); outputStream = fs.create(newPath);
	 * 
	 * IOUtils.copy(inputStream, outputStream,bufferSize); } finally {
	 * inputStream.close(); outputStream.close();
	 * 
	 * fs.close(); } }
	 */
 
    /**
     * 打开HDFS上的文件并返回byte数组
     * @param path
     * @return
     * @throws Exception
     */
    public static byte[] openFileToBytes(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(srcPath);
            return IOUtils.toByteArray(inputStream);
        } finally {
            fs.close();
            inputStream.close();
        }
    }
 
    /**
     * 打开HDFS上的文件并返回java对象
     * @param path
     * @return
     * @throws Exception
     */
    public static <T extends Object> T openFileToObject(String path, Class<T> clazz) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        String jsonStr = readFile(path);
        return JSONObject.parseObject(jsonStr, clazz);
    }
 
    /**
     * 获取某个文件在HDFS的集群位置
     * @param path
     * @return
     * @throws Exception
     */
    public static BlockLocation[] getFileBlockLocations(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        FileStatus fileStatus = fs.getFileStatus(srcPath);
        return fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    }
    
	public static void copyRename(String oldPath, String newPath) {
//            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
		// 获取本地文件系统
		FileSystem fs = null;
		FSDataOutputStream outputStream =null;
		try {
			fs = getFileSystem();
			FileSystem local = fs;
			outputStream = fs.create(new Path(newPath));
			byte [] bs = { (byte)0xEF, (byte)0xBB, (byte)0xBF};
			outputStream.write(bs);
			FileStatus[] status = fs.globStatus(new Path(oldPath + "/part*"));
			Boolean flag = false;
			for (FileStatus fileStatus : status) {
				FSDataInputStream inputStream = local.open(fileStatus.getPath());
				/*BufferedReader d =null;
				if(flag) {
					*//*
					 * d = new BufferedReader(new InputStreamReader(inputStream)); while() { d. }
					 *//*
					inputStream.seek(0);
					inputStream.seekToNewSource(inputStream.getPos());
				}else{
					flag = true; 
				}*/
				IOUtils.copy(inputStream, outputStream);
				IOUtils.closeQuietly(inputStream);
			}
            IOUtils.closeQuietly(outputStream);

//          fs.close();

//			fs(status[0].getPath(), new Path(fs.getHomeDirectory() + newPath));
            System.out.println(
					"============== 复制文件重命名，原始文件目录：" + oldPath + ", 完整路径：" + status[0].getPath() + ", 新文件：" + newPath);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

	}
    
	public static void close() {
		if (null != fileSystem) {
			try {
				fileSystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
    
	public static void listFile(String srcPath, String dstPath) throws Exception {
		// TODO Auto-generated method stub
		FileSystem fs = null;
		FileSystem local = null;
		// 读取配置文件
		Configuration conf = new Configuration();
		// 指定HDFS地址
		URI uri = new URI("hdfs://127.0.0.1:9000");
		fs = FileSystem.get(uri, conf);
		// 获取本地文件系统
		local = FileSystem.getLocal(conf);
		// 获取文件目录
		FileStatus[] listFile = local.globStatus(new Path(srcPath)); //, new RegxAcceptPathFilter("*.tif$")
		// 获取文件路径
		Path[] listPath = FileUtil.stat2Paths(listFile);
		// 输出文件路径
		Path outPath = new Path(dstPath);
		boolean result = fs.isDirectory(outPath);

		if (result == true) {
			// 循环遍历所有文件路径
			for (Path p : listPath) {
				fs.copyFromLocalFile(p, outPath);
			}
		} else {
			fs.mkdirs(outPath);
			System.out.println("创建路径: " + outPath);
			for (Path p : listPath) {
				fs.copyFromLocalFile(p, outPath);
			}
		}
	}
	
	public static void listFileDir(String srcPath) throws Exception {
		// TODO Auto-generated method stub
		FileSystem fs = null;
		FileSystem local = null;
		// 读取配置文件
		Configuration conf = new Configuration();
		// 指定HDFS地址
		URI uri = new URI("hdfs://node3:9000");
		fs = FileSystem.get(uri, conf);
		// 获取本地文件系统
		local = FileSystem.getLocal(conf);
		// 获取文件目录
		FileStatus[] listFile = local.globStatus(new Path(srcPath));//, new RegxAcceptPathFilter("*.tif$")
		// 获取文件路径
		File localPath = new File(srcPath);
		File[] listPath = localPath.listFiles();
		// 输出文件路径
		boolean result = localPath.isDirectory();
		

		if (result == true) {
			// 循环遍历所有文件路径
			for (File p : listPath) {
				if(RegexMatches.accept(p.getName())) {
					System.out.println(p.getName());
				}
			}
		} else {
			System.out.println(localPath.getName());
		}
	}
 
}