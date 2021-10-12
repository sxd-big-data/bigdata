package com.bigdata.controller;

import com.bigdata.service.CouponService;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/coupon")
public class CouponController {
    @Autowired
    CouponService couponService;

    @ApiOperation(value = "劵码表",  notes = "字符串")
    @GetMapping("/syncCouponResourceToHdfs")
    public void syncCouponResourceToHdfs() {
        couponService.syncCouponResourceToHdfs(false, "coupon_resource");
    }

    @ApiOperation(value = "工具表",  notes = "字符串")
    @GetMapping("/syncPromotionToolToHdfs")
    public void syncPromotionToolToHdfs() {
        couponService.syncPromotionToolToHdfs("promotion_tool");
    }

    @ApiOperation(value = "活动表",  notes = "字符串")
    @GetMapping("/syncTreeActivityToHdfs")
    public void syncTreeActivityToHdfs() {
        couponService.syncTreeActivityToHdfs("tree_activity");
    }

    @ApiOperation(value = "活动明细表",  notes = "字符串")
    @GetMapping("/syncSimpleActivityToHdfs")
    public void syncSimpleActivityToHdfs() {
        couponService.syncSimpleActivityToHdfs("simple_activity");
    }


}
