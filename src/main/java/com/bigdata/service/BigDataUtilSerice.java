package com.bigdata.service;

import com.bigdata.hive.baseDao.GoodsRepository;
import com.bigdata.hive.ceDao.VipPurchaseRecordRepository;
import com.bigdata.hive.ceDao.VirgoAccountsRepository;
import com.bigdata.hive.mallDao.*;
import com.bigdata.spark.service.SparkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BigDataUtilSerice {
    @Autowired
    private SparkService sparkService;
    @Autowired
    private PromotionToolRepository promotionToolRepository;
    @Autowired
    private TreeActivityRepository treeActivityRepository;
    @Autowired
    private SimpleActivityRepository simpleActivityRepository;
    @Autowired
    private GoodsRepository goodsRepository;
    @Autowired
    private ParanaItemRepository paranaItemRepository;
    @Autowired
    private ParanaShopCategoryRepository paranaShopCategoryRepository;
    @Autowired
    private ParanaShopCategoryBindingRepository paranaShopCategoryBindingRepository;
    @Autowired
    private OrdersRepository ordersRepository;
    @Autowired
    private OrderLinesRepository orderLinesRepository;
    @Autowired
    private OrderSideEffectRepository orderSideEffectRepository;
    @Autowired
    private EmployOrderSideEffectRepository employOrderSideEffectRepository;
    @Autowired
    private PurchaseOrderRepository purchaseOrderRep;
    @Autowired
    private CouponResourceRepository couponResourceRep;
    @Autowired
    PayJournalRepository payJournalRepositpry;
    @Autowired
    private VipPurchaseRecordRepository vipPurchaseRecordRep;
    @Autowired
    private PromotionSnapshotRepository promotionSnapshotRep;
    @Autowired
    private UcUserRepository ucUserRepository;
    @Autowired
    private UcUserDetailRepository ucUserDetailRepository;
    @Autowired
    private UserBenefitRepository userBenefitRepository;
    @Autowired
    private UserBudgetRepository userBudgetRepository;
    @Autowired
    private UcUserThirdAccountRepository ucUserThirdAccountRep;
    @Autowired
    private ShippAddressRepository shippAddressRepository;
    @Autowired
    private VirgoAccountsRepository virgoAccountsRepository;

    public void getLoadOverData(String tableNmae, String uploadFilePath){

        switch (tableNmae){
            case "promotion_tool":
                promotionToolRepository.loadOverData(uploadFilePath);
                break;
            case "tree_activity":
                treeActivityRepository.loadOverData(uploadFilePath);
                break;
            case "simple_activity":
                simpleActivityRepository.loadOverData(uploadFilePath);
            case "goods":
                goodsRepository.loadOverData(uploadFilePath);
                break;
            case "parana_item":
                paranaItemRepository.loadOverData(uploadFilePath);
                break;
            case "parana_shop_category":
                paranaShopCategoryRepository.loadOverData(uploadFilePath);
                break;
            case "parana_shop_category_binding":
                paranaShopCategoryBindingRepository.loadOverData(uploadFilePath);
                break;
            case "order":
                ordersRepository.loadOverData(uploadFilePath);
                break;
            case "order_line":
                orderLinesRepository.loadOverData(uploadFilePath);
                break;
            case "order_side_effect":
                orderSideEffectRepository.loadOverData(uploadFilePath);
                break;
            case "employ_order_side_effect":
                employOrderSideEffectRepository.loadOverData(uploadFilePath);
                break;
            case "purchase_order":
                purchaseOrderRep.loadOverData(uploadFilePath);
                break;
            case "coupon_resource":
                couponResourceRep.loadOverData(uploadFilePath);
                break;
            case "pay_journal":
                payJournalRepositpry.loadOverData(uploadFilePath);
                break;
            case "vip_purchase_record":
                vipPurchaseRecordRep.loadOverData(uploadFilePath);
                break;
            case "promotion_snapshot":
                promotionSnapshotRep.loadOverData(uploadFilePath);
                break;
            case "uc_user":
                ucUserRepository.loadOverData(uploadFilePath);
                break;
            case "uc_user_detail":
                ucUserDetailRepository.loadOverData(uploadFilePath);
                break;
            case "uc_user_third_account":
                ucUserThirdAccountRep.loadOverData(uploadFilePath);
                break;
            case "user_benefit":
                userBenefitRepository.loadOverData(uploadFilePath);
                break;
            case "user_budget":
                userBudgetRepository.loadOverData(uploadFilePath);
                break;
            case "shipping_address":
                shippAddressRepository.loadOverData(uploadFilePath);
                break;
            case "virgo_accounts":
                virgoAccountsRepository.loadOverData(uploadFilePath);
                break;
        }

    }

    public String queryMaxIdAndCreatedTime(String tableNmae){
        String sql = null;
        switch (tableNmae){
            case "order":
                sql = ordersRepository.queryMaxIdAndCreatedTime();
                break;
            case "order_line":
                sql =  orderLinesRepository.queryMaxIdAndCreatedTime();
                break;
            case "order_side_effect":
                sql = orderSideEffectRepository.queryMaxIdAndCreatedTime();
                break;
            case "employ_order_side_effect":
                sql =   employOrderSideEffectRepository.queryMaxIdAndCreatedTime();
                break;
            case "purchase_order":
                sql =  purchaseOrderRep.queryMaxIdAndCreatedTime();
                break;
            case "coupon_resource":
                sql =  couponResourceRep.queryMaxIdAndCreatedTime();
                break;
            case "pay_journal":
                sql =  payJournalRepositpry.queryMaxIdAndCreatedTime();
                break;
            case "vip_purchase_record":
                sql =  vipPurchaseRecordRep.queryMaxIdAndCreatedTime();
                break;
            case "promotion_snapshot":
                sql =  promotionSnapshotRep.queryMaxIdAndCreatedTime();
                break;
            case "uc_user":
                sql =  ucUserRepository.queryMaxIdAndCreatedTime();
                break;
            case "uc_user_third_account":
                sql =  ucUserThirdAccountRep.queryMaxIdAndCreatedTime();
                break;
            case "user_benefit":
                sql = userBenefitRepository.queryMaxIdAndCreatedTime();
                break;
            case "user_budget":
                sql =  userBudgetRepository.queryMaxIdAndCreatedTime();
                break;
            case "shipping_address":
                sql = shippAddressRepository.queryMaxIdAndCreatedTime();
                break;
            case "virgo_accounts":
                sql = virgoAccountsRepository.queryMaxIdAndCreatedTime();
                break;
        }

        return sql;
    }

    public void createTmpTable(String tableNmae){

        switch (tableNmae){
            case "order":
                 ordersRepository.createTmpTable();
                 break;
            case "order_line":
                orderLinesRepository.createTmpTable();
                break;
//            case "order_side_effect":
//                orderSideEffectRepository.createTmpTable();
//                break;
            case "employ_order_side_effect":
                employOrderSideEffectRepository.createTmpTable();
                break;
//            case "purchase_order":
//                purchaseOrderRep.createTmpTable();
//                break;
//            case "coupon_resource":
//                couponResourceRep.createTmpTable();
//                break;
//            case "pay_journal":
//                payJournalRepositpry.createTmpTable();
//                break;
//            case "vip_purchase_record":
//                vipPurchaseRecordRep.createTmpTable();
//                break;
//            case "promotion_snapshot":
//                promotionSnapshotRep.createTmpTable();
//                break;
//            case "uc_user":
//                ucUserRepository.createTmpTable();
//                break;
//            case "uc_user_third_account":
//                ucUserThirdAccountRep.createTmpTable();
//                break;
//            case "user_benefit":
//                userBenefitRepository.createTmpTable();
//                break;
//            case "user_budget":
//                userBudgetRepository.createTmpTable();
//                break;
//            case "shipping_address":
//                shippAddressRepository.createTmpTable();
//                break;
//            case "virgo_accounts":
//                virgoAccountsRepository.createTmpTable();
//                break;
        }
    }

    public void loadTmpData(String tableNmae, String uploadFilePath){

        switch (tableNmae){
            case "order":
                ordersRepository.loadTmpData(uploadFilePath);
                break;
            case "order_line":
                orderLinesRepository.loadTmpData(uploadFilePath);
                break;
            case "order_side_effect":
                orderSideEffectRepository.loadTmpData(uploadFilePath);
                break;
            case "employ_order_side_effect":
                employOrderSideEffectRepository.loadTmpData(uploadFilePath);
                break;
            case "purchase_order":
                purchaseOrderRep.loadTmpData(uploadFilePath);
                break;
            case "coupon_resource":
                couponResourceRep.loadTmpData(uploadFilePath);
                break;
            case "pay_journal":
                payJournalRepositpry.loadTmpData(uploadFilePath);
                break;
            case "vip_purchase_record":
                vipPurchaseRecordRep.loadTmpData(uploadFilePath);
                break;
            case "promotion_snapshot":
                promotionSnapshotRep.loadTmpData(uploadFilePath);
                break;
            case "uc_user":
                ucUserRepository.loadTmpData(uploadFilePath);
                break;
            case "uc_user_third_account":
                ucUserThirdAccountRep.loadTmpData(uploadFilePath);
                break;
            case "user_benefit":
                userBenefitRepository.loadTmpData(uploadFilePath);
                break;
            case "user_budget":
                userBudgetRepository.loadTmpData(uploadFilePath);
                break;
            case "shipping_address":
                shippAddressRepository.loadTmpData(uploadFilePath);
                break;
            case "virgo_accounts":
                virgoAccountsRepository.loadTmpData(uploadFilePath);
                break;
        }
    }

    public void loadData(String tableNmae, String uploadFilePath){

        switch (tableNmae){
            case "order":
                ordersRepository.loadData(uploadFilePath);
                break;
            case "order_line":
                orderLinesRepository.loadData(uploadFilePath);
                break;
            case "order_side_effect":
                orderSideEffectRepository.loadData(uploadFilePath);
                break;
            case "employ_order_side_effect":
                employOrderSideEffectRepository.loadData(uploadFilePath);
                break;
            case "purchase_order":
                purchaseOrderRep.loadData(uploadFilePath);
                break;
            case "coupon_resource":
                couponResourceRep.loadData(uploadFilePath);
                break;
            case "pay_journal":
                payJournalRepositpry.loadData(uploadFilePath);
                break;
            case "vip_purchase_record":
                vipPurchaseRecordRep.loadData(uploadFilePath);
                break;
            case "promotion_snapshot":
                promotionSnapshotRep.loadData(uploadFilePath);
                break;
            case "uc_user":
                ucUserRepository.loadData(uploadFilePath);
                break;
            case "uc_user_third_account":
                ucUserThirdAccountRep.loadData(uploadFilePath);
                break;
            case "user_benefit":
                userBenefitRepository.loadData(uploadFilePath);
                break;
            case "user_budget":
                userBudgetRepository.loadData(uploadFilePath);
                break;
            case "shipping_address":
                shippAddressRepository.loadData(uploadFilePath);
                break;
            case "virgo_accounts":
                virgoAccountsRepository.loadData(uploadFilePath);
                break;
        }
    }

    public void updateHisData(String tableNmae){
        String  sql[] ;
        switch (tableNmae){
            case "order":
                 sql = ordersRepository.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("OrderSideEffectHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("OrderSideEffectHisUpdate", sql[1]);
                break;
            case "order_line":
                 sql =orderLinesRepository.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("OrderLineHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("OrderLineHisUpdate", sql[1]);
                break;
            case "order_side_effect":
                 sql =orderSideEffectRepository.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("OrderSideEffectHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("OrderSideEffectHisUpdate", sql[1]);
                break;
            case "":
                 sql =employOrderSideEffectRepository.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("EmployOrderSideEffectHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("EmployOrderSideEffectHisUpdate", sql[1]);
                break;
            case "purchase_order":
                 sql =purchaseOrderRep.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("PurchaseOrderRepHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("PurchaseOrderRepHisUpdate", sql[1]);
                break;
            case "coupon_resource":
                sql =couponResourceRep.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("CouponResourceRepHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("CouponResourceRepHisUpdate", sql[1]);
                break;
            case "pay_journal":
                sql =payJournalRepositpry.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("PayJournalRepHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("PayJournalRepHisUpdate", sql[1]);
                break;
            case "vip_purchase_record":
                sql =vipPurchaseRecordRep.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("VipPurchaseRecordHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("VipPurchaseRecordHisUpdate", sql[1]);
                break;
            case "promotion_snapshot":
                sql =promotionSnapshotRep.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("PromotionSnapshotHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("PromotionSnapshotHisUpdate", sql[1]);
                break;
            case "uc_user":
                sql =ucUserRepository.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("UcUserHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("UcUserHisUpdate", sql[1]);
                break;
            case "uc_user_third_account":
                sql =ucUserThirdAccountRep.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("UcUserThirdAccountRepHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("UcUserThirdAccountRepHisUpdate", sql[1]);
                break;
            case "user_benefit":
                sql =userBenefitRepository.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("UserBenefitHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("UserBenefitHisUpdate", sql[1]);
                break;
            case "user_budget":
                sql =userBudgetRepository.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("UserBudgetHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("UserBudgetHisUpdate", sql[1]);
                break;
            case "shipping_address":
                sql =shippAddressRepository.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("ShippingAddressHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("ShippingAddressHisUpdate", sql[1]);
                break;
            case "virgo_accounts":
                sql =virgoAccountsRepository.updateHisData();
                //删除要更新的记录
                sparkService.updateHisData("VirgoAccountsHisUpdate", sql[0]);
                //将要更新的最新记录写入库
                sparkService.updateHisData("VirgoAccountsHisUpdate", sql[1]);
                break;
        }
    }


}
