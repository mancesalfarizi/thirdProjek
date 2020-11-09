package projek3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import static org.apache.spark.sql.functions.*;

public class thirdProject {
    public static void main( String[] args )
    {

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);    
        // buat spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Read JSON File to DataSet")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate(); 

        //read hasil crawling di twitter
        Dataset<Row> df1 = spark.read()
        .format("csv")
        .option("delimiter", ",")
        .option("quote", "\"")
        .option("escape", "\"") 
        .option("header", "true")
        .load("file:///home/mancesalfarizi/COVID19");       

        // twitDataset.show();

        //clensing data
        Dataset<Row> data_bersih = df1.withColumn("tweet", regexp_replace(col("tweet"), "[^\\p{L}\\p{N}\\p{P}\\p{Z}]","" ))
                                .withColumn("username", regexp_replace(col("username"), "b'", ""))
                                .withColumn("username", regexp_replace(col("username"), "'", ""))
                                .withColumn("all_hashtags", regexp_replace(col("all_hashtags"), "[\\[\\]\\']", "" ))
                                .withColumn("all_hashtags", regexp_replace(col("all_hashtags"), "'", "" ));
       
        //data yang bersih diubah ke sql                        
        data_bersih.createOrReplaceTempView("data_tweet");

        //call templete sql yang telah dibuat
        Dataset<Row> call = spark.sql("select * from data_tweet");

        //buat objek hive dari class APP
        thirdProject hive = new thirdProject();

        hive.alldata_covid19(call);

        //membuat dataset memanggil semua row dari tabel data_covid19
        Dataset<Row> semuaData = spark.sql("select * from thirdproject.data_covid19");

        //cleansing data yang sama dari semuadatacovid19
        Dataset<Row> dataTanpaDuplicat = semuaData.dropDuplicates("date","username","languange");

        hive.databersih_covid19(dataTanpaDuplicat);
        hive.tweetIndonesia(dataTanpaDuplicat);
        hive.BahasaYangSeringDigunakan(dataTanpaDuplicat);
        hive.waktuTweetTertinggi(dataTanpaDuplicat);

    
 
    }
    //methode untuk menampung data 
    private void alldata_covid19(Dataset<Row> data){
        Dataset<Row> res = data.select("*");

        res.write().mode("append").format("parquet").saveAsTable("thirdproject.data_covid19");
    }
    //method membuat data bersih dari covid19
    private void databersih_covid19(Dataset<Row> data){
        Dataset<Row> res = data.select("*");

        res.write().mode("overwrite").format("parquet").saveAsTable("thirdproject.databersih_covid19");
    }
    //methode pembuatan table mencari yang berbahasa indonesia
    private void tweetIndonesia(Dataset<Row> data){ 
        Dataset<Row> res = data.select("languange")
        .where("languange = 'in'")
        .groupBy("languange").count()
        .withColumnRenamed("count", "jumlah_orang");

        res.write().mode("overwrite").format("parquet").saveAsTable("thirdProject.tweetIndonesia");
    }

    //methode mencari bahasa yang sering digunakan
    private void BahasaYangSeringDigunakan(Dataset<Row> data){
        Dataset<Row> res = data.select("languange")
        .groupBy("languange").count()
        .orderBy(desc("count"))
        .limit(1)
        .withColumnRenamed("count", "total");

        res.write().mode("overwrite").format("parquet").saveAsTable("thirdproject.BahasaYangSeringDigunakan");
    }
    //methode untuk waktu tweet tertinggi
    private void waktuTweetTertinggi(Dataset<Row> data){
        Dataset<Row> res = data.select("date", "languange")
        .withColumn("tanggal", to_date(col("date")))
        .where("languange = 'en'")
        .groupBy("tanggal","languange").count()
        .withColumnRenamed("count", "total");

        res.write().mode("overwrite").format("parquet").saveAsTable("thirdproject.waktuTweetTertinggi");
    }
}