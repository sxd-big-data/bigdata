package com.bigdata.controller;

import com.bigdata.service.ProductService;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/product")
public class ProductController {
    @Autowired
    private ProductService productService;


    @ApiOperation(value = "产品",  notes = "字符串")
    @GetMapping("/syncGoodsToHdfs")
    public void syncGoodsToHdfs() {
        productService.syncGoodsToHdfs("goods");
    }

    @ApiOperation(value = "商品表",  notes = "字符串")
    @GetMapping("/syncParanaItemToHdfs")
    public void syncParanaItemToHdfs() {
        productService.syncParanaItemToHdfs("parana_item");
    }

    @ApiOperation(value = "前台类目",  notes = "字符串")
    @GetMapping("/syncParanaShopCategoryToHdfs")
    public void syncParanaShopCategoryToHdfs() {
        productService.syncParanaShopCategoryToHdfs("parana_shop_category");
    }

    @ApiOperation(value = "前台类目和商品绑定关系",  notes = "字符串")
    @GetMapping("/syncParanaShopCategoryBindingToHdfs")
    public void syncParanaShopCategoryBindingToHdfs() {
        productService.syncParanaShopCategoryBindingToHdfs("parana_shop_category_binding");
    }
}
