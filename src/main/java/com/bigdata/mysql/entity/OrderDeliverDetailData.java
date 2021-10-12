package com.bigdata.mysql.entity;

import java.math.BigDecimal;

import javax.jdo.annotations.Column;

import com.baomidou.mybatisplus.annotation.TableName;

import lombok.Data;

/**
 * @author kongxiangdong
 * @ClassName: OrderDeliverDetailData
 * @Description: 发货记录
 * @date 2016年10月27日 下午5:00:55
 */
@Data
@TableName("order_deliver_detail_data")
public class OrderDeliverDetailData implements Cloneable {
	
	
    private Long detailDataId;

    @Column(length = 30)
    private String orderId;

    @Column(length = 30)
    private String externalOrderId;

    @Column(length = 30)
    private String orderItemSeqId;

    @Column(length = 30)
    private String externalSubOrderId;

    @Column(length = 30)
    private String productId;

    @Column(length = 30)
    private String crmProductId;

    private BigDecimal unitPrice;

    private BigDecimal discountPrice;

    @Column(length = 30)
    private String blockId;

    @Column(length = 30)
    private String belongPartyId;

    @Column(length = 30)
    private String milkmanLoginId;

    @Column(length = 30)
    private String milkmanPartyId;

    @Column(length = 255)
    private String deliverMethod;

    @Column(length = 20)
    private String deliverMonth;

    @Column(length = 20)
    private String deliverDate;

    /**
     * 订单项每日应得积分
     */
    private Short dailyPoints;

    @Column(length = 20)
    private String deliverNum;

    private Long orderNum;

    @Column(length = 20)
    private String operatorType;

    @Column(length = 1000)
    private String operatorComment;

    @Column(length = 30)
    private String createBy;

    @Column(length = 30)
    private String updateBy;

    @Column(length = 1)
    private String isHedge;

    @Column(length = 20)
    private String isAddDiff;

    /**
     * 每日订单金额
     */
    private BigDecimal orderTotalAmount;

    /**
     * 每日订单金额-折扣金额
     */
    private BigDecimal orderSalesAmount;

    /**
     * 每日应支付金额
     */
    private BigDecimal orderPayment;

    /**
     * 商家优惠金额
     */
    private BigDecimal discountAmount;

    @Column(length = 20)
    private String orderStatus;

    //是否已同步给第三方
    @Column(length = 1)
    private String isSync;

    //是否已配送
    @Column(length = 1)
    private String isSend;

    private String cityCode;

    private String contactMechId;

    private String skuId;

    private String createdBy;

    public Long getDetailDataId() {
        return detailDataId;
    }

    public void setDetailDataId(Long detailDataId) {
        this.detailDataId = detailDataId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderItemSeqId() {
        return orderItemSeqId;
    }

    public void setOrderItemSeqId(String orderItemSeqId) {
        this.orderItemSeqId = orderItemSeqId;
    }

    public String getExternalOrderId() {
        return externalOrderId;
    }

    public void setExternalOrderId(String externalOrderId) {
        this.externalOrderId = externalOrderId;
    }

    public String getExternalSubOrderId() {
        return externalSubOrderId;
    }

    public void setExternalSubOrderId(String externalSubOrderId) {
        this.externalSubOrderId = externalSubOrderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getCrmProductId() {
        return crmProductId;
    }

    public void setCrmProductId(String crmProductId) {
        this.crmProductId = crmProductId;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }

    public BigDecimal getDiscountPrice() {
        return discountPrice;
    }

    public void setDiscountPrice(BigDecimal discountPrice) {
        this.discountPrice = discountPrice;
    }

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public String getBelongPartyId() {
        return belongPartyId;
    }

    public void setBelongPartyId(String belongPartyId) {
        this.belongPartyId = belongPartyId;
    }

    public String getMilkmanLoginId() {
        return milkmanLoginId;
    }

    public void setMilkmanLoginId(String milkmanLoginId) {
        this.milkmanLoginId = milkmanLoginId;
    }

    public String getMilkmanPartyId() {
        return milkmanPartyId;
    }

    public void setMilkmanPartyId(String milkmanPartyId) {
        this.milkmanPartyId = milkmanPartyId;
    }

    public String getDeliverMethod() {
        return deliverMethod;
    }

    public void setDeliverMethod(String deliverMethod) {
        this.deliverMethod = deliverMethod;
    }

    public String getDeliverMonth() {
        return deliverMonth;
    }

    public void setDeliverMonth(String deliverMonth) {
        this.deliverMonth = deliverMonth;
    }

    public String getDeliverDate() {
        return deliverDate;
    }

    public void setDeliverDate(String deliverDate) {
        this.deliverDate = deliverDate;
    }

    public String getDeliverNum() {
        return deliverNum;
    }

    public void setDeliverNum(String deliverNum) {
        this.deliverNum = deliverNum;
    }

    public Short getDailyPoints() {
        return dailyPoints;
    }

    public void setDailyPoints(Short dailyPoints) {
        this.dailyPoints = dailyPoints;
    }

    public Long getOrderNum() {
        return orderNum;
    }

    public void setOrderNum(Long orderNum) {
        this.orderNum = orderNum;
    }

    public String getOperatorType() {
        return operatorType;
    }

    public void setOperatorType(String operatorType) {
        this.operatorType = operatorType;
    }

    public String getOperatorComment() {
        return operatorComment;
    }

    public void setOperatorComment(String operatorComment) {
        this.operatorComment = operatorComment;
    }

    public String getCreateBy() {
        return createBy;
    }

    public void setCreateBy(String createBy) {
        this.createBy = createBy;
    }

    public String getUpdateBy() {
        return updateBy;
    }

    public void setUpdateBy(String updateBy) {
        this.updateBy = updateBy;
    }

    public String getIsHedge() {
        return isHedge;
    }

    public void setIsHedge(String isHedge) {
        this.isHedge = isHedge;
    }

    public String getIsAddDiff() {
        return isAddDiff;
    }

    public void setIsAddDiff(String isAddDiff) {
        this.isAddDiff = isAddDiff;
    }

    public BigDecimal getOrderTotalAmount() {
        return orderTotalAmount;
    }

    public void setOrderTotalAmount(BigDecimal orderTotalAmount) {
        this.orderTotalAmount = orderTotalAmount;
    }

    public BigDecimal getOrderSalesAmount() {
        return orderSalesAmount;
    }

    public void setOrderSalesAmount(BigDecimal orderSalesAmount) {
        this.orderSalesAmount = orderSalesAmount;
    }

    public BigDecimal getOrderPayment() {
        return orderPayment;
    }

    public void setOrderPayment(BigDecimal orderPayment) {
        this.orderPayment = orderPayment;
    }

    public BigDecimal getDiscountAmount() {
        return discountAmount;
    }

    public void setDiscountAmount(BigDecimal discountAmount) {
        this.discountAmount = discountAmount;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getIsSync() {
        return isSync;
    }

    public void setIsSync(String isSync) {
        this.isSync = isSync;
    }

    public String getIsSend() {
        return isSend;
    }

    public void setIsSend(String isSend) {
        this.isSend = isSend;
    }

    public String getCityCode() {
        return cityCode;
    }

    public void setCityCode(String cityCode) {
        this.cityCode = cityCode;
    }

    public String getContactMechId() {
        return contactMechId;
    }

    public void setContactMechId(String contactMechId) {
        this.contactMechId = contactMechId;
    }

    public String getSkuId() {
        return skuId;
    }

    public void setSkuId(String skuId) {
        this.skuId = skuId;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @Override
    public OrderDeliverDetailData clone() {
        try {
            return (OrderDeliverDetailData) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
