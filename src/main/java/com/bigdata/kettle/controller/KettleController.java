package com.bigdata.kettle.controller;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.validation.Valid;
import javax.websocket.server.PathParam;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.bigdata.common.ResponseMsg;
import com.bigdata.kettle.service.KettleService;

import cn.hutool.core.io.FileUtil;



@RestController
@RequestMapping("/kettle")
public class KettleController {
	
	@Autowired
	private KettleService kettleService;
	
    @GetMapping("/testKettle")
    public String testKettle() throws KettleException {

        // 可以把文件放到resources目录下面，然后用hutool读取这文件
        // File file = FileUtil.file("33.ktr");
        // 这是我本地进行测试的，所以我把文件放到了桌面上了，写了根目录
        File file = FileUtil.file("D:\\data\\test_kettle\\test_demo.ktr");
        String path = file.getPath();
        StepPluginType.getInstance().getPluginFolders().add(new PluginFolder("E:\\test_boot_workspace\\kettle-test\\plugins\\steps\\pentaho-kafka-producer", false, true));
        // 初始化
        EnvUtil.environmentInit();

        KettleEnvironment.init();
        // 加载文件
        TransMeta transMeta = new TransMeta(path);
        Trans trans = new Trans(transMeta);
        // 放入参数，这里其实可以从数据库中取到
        // 如果没有参数，可以把这步忽略
//        trans.setParameterValue("stade", "2019-04-24");
        trans.prepareExecution(null);
        trans.startThreads();
        // 等待执行完毕
        trans.waitUntilFinished();
        return "success";
    }
    
    
	@GetMapping("/createdTransMeta")
	public String createdTransMeta(String fileName) throws KettleException {
		FileOutputStream out = null;
		try {
			KettleEnvironment.init();
			TransMeta transMeta = kettleService.generateMyOwnTrans();
			String transXml = transMeta.getXML();
			String transName = "E:\\test_boot_workspace\\kettle-test\\update_insert_Trans.ktr";
			File file = new File(transName);
			out = new FileOutputStream(file);
			out.write(transXml.getBytes());

		} catch (Exception e) {
			e.printStackTrace();
			return "fail";
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return "success";
	}
    
	
	@PostMapping("patientTrans")
	public ResponseMsg patientTrans(String ktrFileName,@RequestBody Map<String,String> paramMap)//@RequestBody RequestMsg in,
			throws RuntimeException {
//		String ktrFileName = "base_service_milk_station.ktr";
		ResponseMsg response = this.kettleService.runKtr(ktrFileName, paramMap);
		return response;
	}
	  
	@RequestMapping(value = "patientIn", method = RequestMethod.POST)
//	@InParamCheck
	public ResponseMsg patientIn() //@Valid BindingResult result
			throws RuntimeException {
		String fileName = "patientIn.kjb";
		ResponseMsg response = this.kettleService.runKjb(fileName, new HashMap<>());
		return response;
	}
	 
}