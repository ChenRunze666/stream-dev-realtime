package com.bg.realtime_dwd.base_db.APP;

import com.bg.common.base.BaseSQLApp;
import com.bg.common.constant.Constant;
import com.bg.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.bg.realtime_dwd.base_db.app.DwdInteractionCommentInfo
 * @Author Chen.Run.ze
 * @Date 2025/4/10 19:05
 * @description: 评论事实表
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从kafka的topic_db 主题中读取数据 创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        //TODO 过滤出评论数据
        Table commentInfo = tableEnv.sqlQuery( "select " +
                "`after`['id'] id," +
                "`after`['user_id'] user_id," +
                "`after`['sku_id'] sku_id," +
                "`after`['appraise'] appraise," +
                "`after`['comment_txt'] comment_txt," +
                "`ts_ms`," +
                "`proc_time` " +
                "from topic_db where `source`['table'] = 'comment_info' ");
//        commentInfo.execute().print();
        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info",commentInfo);

        //TODO 从HBase中读取字典数据 创建动态表
        readBaseDic(tableEnv);

        //TODO 将评论表和字典表进行关联
        Table joinedTable = tableEnv.sqlQuery("SELECT id,user_id,sku_id,appraise,dic.dic_name appraise_name,comment_txt,ts_ms FROM comment_info as c \n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time as dic\n" +
                "ON c.appraise = dic.dic_code;");
//        joinedTable.execute().print();

        //TODO 将关联的结果写到kafka 主题中
        //7.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO +" (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  appraise STRING,\n" +
                "  appraise_name STRING,\n" +
                "  comment_txt STRING,\n" +
                "  ts_ms BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        //7.2 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }


}
