package com.bigdata.controller;

import com.bigdata.service.UserService;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/user")
public class UserContoroller {

    @Autowired
    UserService userService;

    @ApiOperation(value = "用户",  notes = "字符串")
    @GetMapping("/syncUcUserToHdfs")
    public void syncOrderToHdfs() {
        userService.syncUcUserToHdfs(false, "uc_user");

    }

    @ApiOperation(value = "用户扩展信息",  notes = "字符串")
    @GetMapping("/syncUcUserDetailToHdfs")
    public void syncUcUserDetailToHdfs() {
        userService.syncUcUserDetailToHdfs( "uc_user_detail");

    }

    @ApiOperation(value = "第三方用户",  notes = "字符串")
    @GetMapping("/syncUcUserThirdAccountToHdfs")
    public void syncUcUserThirdAccountToHdfs() {
        userService.syncUcUserThirdAccountToHdfs( false,"uc_user_third_account");

    }


    @ApiOperation(value = "用户——优惠劵表",  notes = "字符串")
    @GetMapping("/syncUserBenefitToHdfs")
    public void syncUserBenefitToHdfs() {
        userService.syncUserBenefitToHdfs( false,"user_benefit");

    }

    @ApiOperation(value = "员工折扣额度表",  notes = "字符串")
    @GetMapping("/syncUserBudgetToHdfs")
    public void syncUserBudgetToHdfs() {
        userService.syncUserBudgetToHdfs( false,"user_budget");

    }

    @ApiOperation(value = "地址表",  notes = "字符串")
    @GetMapping("/syncShippingAddressToHdfs")
    public void syncShippingAddressToHdfs() {
        userService.syncShippingAddressToHdfs( false,"shipping_address");

    }
}
