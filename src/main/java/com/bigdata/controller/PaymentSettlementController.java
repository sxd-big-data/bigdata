package com.bigdata.controller;

import com.bigdata.common.ResponseMsg;
import com.bigdata.service.PaymentSettlementDataService;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/pay")
public class PaymentSettlementController {
    @Autowired
    PaymentSettlementDataService paymentSettlementDataService;


    @ApiOperation(value = "支付退款流水",  notes = "字符串")
    @GetMapping("syncPayJournalToHdfs")
    public void syncPayJournalToHdfs(){
         paymentSettlementDataService.syncPayJournalToHdfs(false, "pay_journal");
    }

}
