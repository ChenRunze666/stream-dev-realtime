package com.bg.realtime_dwd.base_db.APP;

import com.bg.common.base.BaseSQLApp;
import com.bg.common.constant.Constant;
import com.bg.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.bg.realtime_dwd.base_db.app.DwdTradeOrderRefund
 * @Author Chen.Run.ze
 * @Date 2025/4/11 15:36
 * @description: 退单事实表
 */
public class DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(10017, 4, Constant.TOPIC_DWD_TRADE_ORDER_REFUND
        );

    }

    @Override
    public void handle(StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // 1.1 读取 topic_db
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
        // 1.2 读取 字典表
        readBaseDic(tEnv);

        // 2. 过滤退单表数据 order_refund_info   insert
        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['order_id'] order_id," +
                        "after['sku_id'] sku_id," +
                        "after['refund_type'] refund_type," +
                        "after['refund_num'] refund_num," +
                        "after['refund_amount'] refund_amount," +
                        "after['refund_reason_type'] refund_reason_type," +
                        "after['refund_reason_txt'] refund_reason_txt," +
                        "after['create_time'] create_time," +
                        "pt," +
                        "ts_ms " +
                        "from topic_db " +
                        "where `source`['table']='order_refund_info' " +
                        "and `op`='c' ");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 3. 过滤订单表中的退单数据: order_info  update
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['province_id'] province_id," +
                        "`before` " +
                        "from topic_db " +
                        "where `source`['table']='order_info' " +
                        "and `op`='u'" +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1005' ");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 4. join: 普通的和 lookup join
        Table result = tEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(ri.create_time AS BIGINT), 3), 'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts_ms " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.pt as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.pt as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");

        // 5. 写出到 kafka
        tEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_REFUND+"(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts_ms bigint ," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));
//        result.execute().print();
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);


    }

}