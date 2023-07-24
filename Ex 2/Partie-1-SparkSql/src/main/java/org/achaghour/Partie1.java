package org.achaghour;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Partie1 {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Flights")
                .master("local[*]")
                .getOrCreate();

        String jdbcUrl = "\"jdbc:mysql://localhost:3306/DB_AEROPORT\"";
        Dataset<Row> volsDF = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "vols")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> reservationsDF = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "reservations")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> passagersDF = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "passangers")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> flightPassengerCountDF = volsDF
                .join(reservationsDF, volsDF.col("ID_VOL").equalTo(reservationsDF.col("ID_VOL")))
                .join(passagersDF, reservationsDF.col("ID_PASSAGER").equalTo(passagersDF.col("ID_PASSAGER")))
                .groupBy(volsDF.col("ID_VOL"), volsDF.col("DATE_DEPART"))
                .agg(org.apache.spark.sql.functions.count(passagersDF.col("ID_PASSAGER")).alias("NOMBRE"))
                .orderBy(volsDF.col("ID_VOL"));

        // Show the results
        flightPassengerCountDF.show();

        Dataset<Row> ongoingFlightsDF = spark.read()
                .format("jdbc")
                .option("driver", jdbcUrl)
                .option("url", "jdbc:mysql://localhost:3306/emp")
                .option("dbtable", "SELECT ID_VOL, DATE_DEPART, DATE_ARRIVE FROM VOLS WHERE DATE_DEPART >= CURRENT_DATE")
                .option("user", "root")
                .option("password", "")
                .load();
        // Show the results
        ongoingFlightsDF.show();

        spark.stop();
    }
}