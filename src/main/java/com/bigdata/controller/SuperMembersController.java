package com.bigdata.controller;

import com.bigdata.service.SuperMembersService;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/super")
public class SuperMembersController {
    @Autowired
    SuperMembersService superMembersService;

    @ApiOperation(value = "超级会员购买记录",  notes = "字符串")
    @GetMapping("/syncVipPurchaseRecordToHdfs")
    public void syncVipPurchaseRecordToHdfs() {
        superMembersService.syncVipPurchaseRecordToHdfs(false, "vip_purchase_record");
    }

    @ApiOperation(value = "超级会员优惠信息",  notes = "字符串")
    @GetMapping("/syncPromotionSnapshotToHdfs")
    public void syncPromotionSnapshotToHdfs() {
        superMembersService.syncPromotionSnapshotToHdfs(false, "promotion_snapshot");
    }
}
