package com.bigdata.spark.utils;

import java.util.List;

public class ParamValidateUtil {

    static String SUCCESS = "success";
    public static String checkParam(List<String> param, boolean canEmpty, int count, boolean optional) {
        String msg = SUCCESS;
        if(!canEmpty) {
            msg = (null == param || 0 == param.size()) ? "参数不能为空": SUCCESS;
            if(!SUCCESS.equals(msg)) {
                return msg;
            }
            msg = count <= 0 ? "参数数量和非空限制冲突" : SUCCESS;
            if(!SUCCESS.equals(msg)) {
                return msg;
            }
            // 数量为必选时，必须等于指定值
            if((optional && param.size() < count) || (!optional && param.size() != count)) {
                msg = "参数数量和要求不匹配";
            }
        }
        return msg;
    }

    public static boolean passValid(String result) {
        if(SUCCESS.equals(result)) {
            return true;
        }
        return false;
    }
}
