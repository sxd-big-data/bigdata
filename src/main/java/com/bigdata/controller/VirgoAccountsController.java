package com.bigdata.controller;

import com.bigdata.service.VirgoAccountService;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/integral")
public class VirgoAccountsController {
   @Autowired
    VirgoAccountService virgoAccountService;


    @ApiOperation(value = "会员积分",  notes = "字符串")
    @GetMapping("/syncVirgoAccountsToHdfs")
    public void syncOrderToHdfs() {
        // TODO Auto-generated method stub
        virgoAccountService.syncVirgoAccountsToHdfs(false, "virgo_accounts");

    }
}
