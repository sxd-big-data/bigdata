package com.bigdata.spark.constons;

public enum SqlConstEnum {

    BASE_ORG("base_org", "(select a.key block_id, a.biz_name block_name, b.key area_id, b.biz_name area_name " +
            "      , c.key city_id, c.biz_name city_name " +
            "    from acl_tree_node a , acl_tree_node b , acl_tree_node c " +
            "   where a.parent_key = b.key and b.parent_key = c.key " +
            "   and a.biz_type = 'milk_station') base_org "
            , "owt")
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
