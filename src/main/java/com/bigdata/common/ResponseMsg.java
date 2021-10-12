package com.bigdata.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 统一返回参数外部结构包装类
 * Created by kongxiangdong on 2017/5/18 16:31.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ResponseMsg<T> {
    /**
     * 返回代码
     */
    private String msgCode;

    /**
     * 返回内容
     */
    private String msgText;

    /**
     * 返回结果
     */
    private T result;

    public ResponseMsg() {}

    private ResponseMsg(String msgCode, String msgText) {
        this.msgCode = msgCode;
        this.msgText = msgText;
    }

    public static <T> ResponseMsg<T> getSuccessMsg() {
        return getMsg(MessageEnum.MSG_SUCCESS);
    }
    
    public static <T> ResponseMsg<T> getFailMsg() {
        return getMsg(MessageEnum.MSG_ERROR);
    }

    public static <T> ResponseMsg<T> getMsg(com.bigdata.common.MessageEnum msgError) {
        return new ResponseMsg<T>(msgError.getMsgCode() ,msgError.getMsgText());
    }

    public void setMsg(BaseMsg msg) {
        this.msgCode = msg.getMsgCode();
        this.msgText = msg.getMsgText();
    }

    @JsonIgnore
    public boolean isSuccess(){
        if(MessageEnum.MSG_SUCCESS.getMsgCode().equals(getMsgCode())) {
            return true;
        }

        return false;
    }

    public String getMsgCode() {
        return msgCode;
    }

    public void setMsgCode(String msgCode) {
        this.msgCode = msgCode;
    }

    public String getMsgText() {
        return msgText;
    }

    public void setMsgText(String msgText) {
        this.msgText = msgText;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }
    
	/*
	 * @Override public String toString() { return JsonUtil.toJsonStr(this); }
	 */
}
