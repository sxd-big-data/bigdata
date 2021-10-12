package com.bigdata.hadoop.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.bigdata.hadoop.utils.HDFSUtils;
 
/**
 * @ClassName HDFSController
 * @Date 2020/8/26 11:41
 */
@RestController
@RequestMapping("/spark/hdfs")
public class HDFSController {
 
    @GetMapping("/saveFileToLocal")
    public String saveFileToLocal(String fileName,String outPathName) throws Exception {
        return HDFSUtils.saveFileToLocal(fileName,outPathName);
    }
 
    @PostMapping("/upload")
    public void upload(MultipartFile file) throws Exception {
        HDFSUtils.createFile(file);
    }
    
    
    @PostMapping("/uploadFromFileName")
    public void uploadFromFileName(String fileName) throws Exception {
        HDFSUtils.createFile(fileName);
    }
    
    @PostMapping("/rename")
    public void rename(String oldName,String newName) throws Exception {
        HDFSUtils.renameFile(oldName, newName);
    }
    
    @PostMapping("/uploadFile")
    public void uploadFile(String localPath,String uploadPath) throws Exception {
        HDFSUtils.uploadFile(localPath, uploadPath);
    }
    
    @PostMapping("/uploadFileByName")
    public void uploadFile(String fileName) throws Exception {
        HDFSUtils.uploadFile(fileName);
    }
    
    @PostMapping("/listFile")
    public void listFile(String path) throws Exception {
        HDFSUtils.listFile(path);
    }
    
    @PostMapping("/listFileDir")
    public void listFileDir(String path) throws Exception {
        HDFSUtils.listFileDir(path);
    }
    
}