package edu.zju.gis.hls.test.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author Zhou
 * @date 2020/7/13
 * MySQL 读写测试
 **/
@Slf4j
public class MysqlExample {

    public static void main(String[] args){
        // setup spark environment
        SparkSession ss = SparkSession
                .builder()
                .appName("MySQL Read Demo")
                .master("local[4]")
                .getOrCreate();
        System.out.println("数据库设置成功");

        String tablename = "test1";

        Dataset<Row> inputDataSet = ss.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/test_db?serverTimezone=Asia/Shanghai")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "566000")
                .option("dbtable", tablename)
                .option("pushDownPredicate", true).load();
        System.out.println("数据库读取成功");

        inputDataSet.show();

        String[] colums = inputDataSet.columns();
        for (String col: colums) {
            System.out.print(col + ";");
        }

        inputDataSet.cache();

        inputDataSet = inputDataSet.select("id", "name", "wkt");

        System.out.println(" ====== SHOW ====== ");
        inputDataSet.show();

        System.out.println(" ====== EXPLAIN ====== ");
        inputDataSet.explain(true);

        System.out.println(" ====== FINISH ====== ");
        log.info("Read Success with total count number: " + inputDataSet.count());

        System.out.println(" ====== LAYER WRITE TEST ===== ");
        inputDataSet.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/test_db?serverTimezone=Asia/Shanghai")
                .option("dbtable", "test2")
                .option("user", "root")
                .option("password", "566000")
                .save();

        ss.close();
    }
}
