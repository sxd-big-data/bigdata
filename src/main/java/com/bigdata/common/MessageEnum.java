package com.bigdata.common;

/**
 * 消息代码以及消息描述
 * Created by kongxiangdong on 2017/5/18 17:01.
 */
public enum MessageEnum {
    MSG_SUCCESS("000","success") ,
    MSG_PARAM_EMPTY("001", "参数存在空值") ,
    MSG_404("404" ,"请求地址在火星ㄟ( ▔, ▔ )ㄏ") ,
    MSG_PIN_ERROR("990" ,"请求参数校验失败"),
    MSG_GEO_MAP_NOTRUND("612","地图信息没找到"),
    MSG_ERROR("999" ,"出错了呢(*´Д｀*)")
    ;

    MessageEnum(String msgCode, String msgText) {
        this.msgCode = msgCode;
        this.msgText = msgText;
    }

    private String msgCode;

    private String msgText;
    public String getMsgCode() {
        return this.msgCode;
    }

    public String getMsgText() {
        return this.msgText;
    }
}
