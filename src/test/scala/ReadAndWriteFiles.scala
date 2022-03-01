import com.jumpdd.dataLake.core.Spark
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

class ReadAndWriteFiles extends AnyFunSuite with should.Matchers with BeforeAndAfter{

  //VARIABLES WITH THE FILE PATHS
  //IMPORTANT DO NOT FORGET TO CHANGE THE PATH IF YOU MODIFY THEM

  val pathReadParquet = "seeds/raw/accedo_one/catalogue"
  val pathWriteParquet = " "
  val readParquetFile = "seeds/fake_raw/accedo_one/playbackactivity"
  val pathReadJson = " "
  val pathWriteJson = "seeds/raw_json/catalogue"
  val pathReadCsv = " "
  val pathWriteCsv = " "
  val partition1 = ""
  val partition2 = ""



  test("CONVERT PARQUET TO JSON"){
    val spark = Spark.configureSparkSession()
    val df = spark.read.parquet(pathReadParquet)
    df.printSchema()
    df.show(false)
    df.write.mode(SaveMode.Overwrite).json(pathWriteJson)
  }
  test("CONVERT JSON TO PARQUET WITH PARTITIONS"){
    val spark = Spark.configureSparkSession()
    val df = spark.read.json(pathReadJson)
    df.show(false)
    df.printSchema()
    df.write.partitionBy(partition1,partition2).mode(SaveMode.Overwrite).parquet(pathWriteParquet)
  }

  test("CONVERT JSON TO PARQUET"){
    val spark = Spark.configureSparkSession()
    val df = spark.read.json(pathReadJson)
    df.show(false)
    df.printSchema()
    df.write.mode(SaveMode.Overwrite).parquet(pathWriteParquet)
  }





  test("CONVERT PARQUET TO CSV"){
    val spark = Spark.configureSparkSession()
    val df = spark.read.parquet(pathReadParquet)
    df.show(false)
    df.printSchema()
    df.write.mode(SaveMode.Overwrite).option("header", "true").csv(pathWriteCsv)
  }
  test("CONVERT CSV TO PARQUET"){
    val spark = Spark.configureSparkSession()
    val df = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(pathReadCsv)
    df.show(false)
    df.printSchema()
    df.write.mode(SaveMode.Overwrite).parquet(pathWriteParquet)
  }
  test("READ PARQUET FILE PATH"){
    val spark = Spark.configureSparkSession()
    val df = spark.read.parquet(readParquetFile)
    df.printSchema()
    df.show(false)
  }
}