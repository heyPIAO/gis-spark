package edu.zju.gis.hls.test.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hu
 * @date 2020/7/7
 **/
public class PgReadExample {

  private static final Logger logger = LoggerFactory.getLogger(PgReadExample.class);

  public static void main(String[] args) {

    // setup spark environment
    SparkSession ss = SparkSession
      .builder()
      .appName("Postgresql Read Demo")
      .master("local[4]")
      .getOrCreate();

    String schema = "public";
    String tablename = "test";

    Dataset<Row> inputDataSet = ss.read().format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("user", "postgres")
      .option("password", "root")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", String.format("%s.%s", schema, tablename))
      .option("pushDownPredicate", true).load();

    String[] colums = inputDataSet.columns();
    for (String col: colums) {
      System.out.print(col + ";");
    }

    inputDataSet = inputDataSet.select("id", "shape");

    System.out.println(" ====== SCHEMA ====== ");
    inputDataSet.printSchema();

    System.out.println(" ====== SHOW ====== ");
    inputDataSet.show();

    System.out.println(" ====== EXPLAIN ====== ");
    inputDataSet.explain(true);

    System.out.println(" ====== FINISH ====== ");
    logger.info("Read Success with total count number: " + inputDataSet.count());

  }


}
