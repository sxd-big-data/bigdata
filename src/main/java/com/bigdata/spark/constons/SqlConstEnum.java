package com.bigdata.spark.constons;

public enum SqlConstEnum {

    BASE_ORG("base_org", "(select a.key block_id, a.biz_name block_name, b.key area_id, b.biz_name area_name " +
            "      , c.key city_id, c.biz_name city_name " +
            "    from `t-acl`.acl_tree_node a , `t-acl`.acl_tree_node b , `t-acl`.acl_tree_node c " +
            "   where a.parent_key = b.key and b.parent_key = c.key " +
            "   and a.biz_type = 'milk_station') base_org "
            , "owt"),

    ERP_INFO("erp_info", "(" +
            " select   a.id erp_id, a.name erp_name, a.customer_id, b.name customer_name  , a.org_id, a.org_name " +
            "        from base_service.erp_net_point a , base_service.erp_customer b , base_service.erp_organization c " +
            "        where a.customer_id = b.id and a.org_id = c.id) erp"
            , "owt"),

    FULFILL_ORDER_GOODS("fulfill_goods", "(" +
                        " SELECT  o.id    order_id , o.milkStationId, " +
                        " o.milkStationName  ," +
                        " o.outOrderId     , " +
                        " o.createdAt  ," +
                        " o.operatorType," +
                        " o.discountAmount/100 discountAmount," +
                        " g.goodsName  , g.predictDeliveryAt, " +
                        " g.noticeQuantity       ," +
                        " g.originAmount/100 originAmountPerDay," +
                        " g.paidAmount / 100     paidAmountPerDay  ," +
                        " JSON_UNQUOTE(o.demandSideAddress -> '$.fullAddress') as shipAddr," +
                        " o.demandSideMobile      shipMobile   ," +
                        " o.demandSideName         userName         ," +
                        " o.demandSideContact            shipToName," +
                        " CASE o.deliveryType WHEN 'MORNING' THEN '晨配' ELSE '夜配' END AS       deliveryType," +
                        " o.subChannelName           ," +
                        " o.deliverymanName             ," +
                        " o.originAmount/100 originAmountOrder," +
                        " o.paidAmount/100 paidAmountOrder," +
                        " CASE o.auditStatus WHEN 'AUDITED' THEN '已审核'    WHEN 'CLOSED' THEN '已关闭' ELSE '等待中' END auditStatus," +
                        " CASE o.fulfillmentStatus WHEN 'PENDING' THEN '等待中'    WHEN 'BREAKUP' THEN '已终止'  ELSE '已处理' END AS fulfillmentStatusOrder, " +
                        " g.fulfillmentStatus fulfillmentStatusGoods" +
                        " FROM owt_oms.fulfillment_order o " +
                        " INNER JOIN owt_oms.fulfillment_goods g ON o.id = g.fulfillmentOrderId" +
                        " WHERE " +
                        " o.type = 'SALE_ORDER'" +
                        " AND o.fulfillmentStatus <> 'BREAKUP' " +
                        " AND o.createdAt >= '{0}' " +
                        " AND o.createdAt < '{1}' " +
                        " and g.fulfillmentStatus <> 'BREAKUP' " +
                        " ) goods", "owt"),

    FULFILL_GOODS_SUM("fulfil_goods_sum", "SELECT  order_id     ,   " +
            "  city_name," +
            "  area_name, " +
            "       milkStationName  ," +
            "      outOrderId     ," +
            "       createdAt  ," +
            "       operatorType, " +
            "  subChannelName ," +
            "      discountAmount," +
            "       goodsName  ," +
            "       noticeQuantity  ," +
            "       originAmountPerDay, " +
            "       shipAddr, " +
            "        shipMobile   ," +
            "        userName         ," +
            "        shipToName," +
            "       MIN(predictDeliveryAt)  ,MAX(predictDeliveryAt) ," +
            "        deliveryType," +
            "       subChannelName           ," +
            "       deliverymanName             ," +
            "       originAmountOrder," +
            "       paidAmountOrder," +
            "       auditStatus," +
            "    fulfillmentStatusOrder," +
            "    fulfillmentStatusGoods" +
            " from fulfill_goods" +
            " join base_org on base_org.block_id =     fulfill_goods.milkStationId" +
            " group by order_id  ,  city_name,area_name,   milkStationName  ,  outOrderId  , createdAt  , operatorType, " +
            "   subChannelName , discountAmount,  goodsName  ,   noticeQuantity  , originAmountPerDay, " +
            "       shipAddr" +
            ",   shipMobile   ,  userName  ,   shipToName,deliveryType,   subChannelName           ," +
            "       deliverymanName  ,   originAmountOrder,   paidAmountOrder,   auditStatus,   fulfillmentStatusOrder,  fulfillmentStatusGoods", "owt"),
    ;

    private String sqlName;
    private String sqlStr;
    private String dbName;

    SqlConstEnum(String sqlName, String sqlStr, String dbName) {
        this.sqlName = sqlName;
        this.sqlStr = sqlStr;
        this.dbName = dbName;
    }

    public String getSqlName() {
        return sqlName;
    }

    public String getSqlStr() {
        return sqlStr;
    }

    public String getDbName() {
        return dbName;
    }

    public static void main(String[] args) {
        System.out.println(SqlConstEnum.BASE_ORG.sqlName);
        System.out.println(SqlConstEnum.BASE_ORG.sqlStr);
    }
}
