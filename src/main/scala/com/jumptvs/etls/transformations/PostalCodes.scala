package com.jumptvs.etls.transformations

import cats.effect.IO
import com.jumptvs.etls.config.GlobalConfiguration
import com.jumptvs.etls.db.{DB, Info}
import com.jumptvs.etls.model.m7.M7
import doobie.util.transactor.Transactor
import org.apache.spark.sql.functions.{col, regexp_extract, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PostalCodes {

  def getPostalCodes(config: GlobalConfiguration)(implicit sparkSession: SparkSession):DataFrame = {
    import sparkSession.implicits._
    val xaPostalcode: Transactor[IO] = DB.getConnectionPostalCodes(config)
    val listaCodigos = Info.readInfoFromTablePostalCode(xaPostalcode)

    val postalCodeDF =
    sparkSession
      .sparkContext
      .parallelize(listaCodigos)
      .toDF(M7.postalCodePostalCode, M7.countryCodePostalCode, M7.regionCodePostalCode)
 /*     .withColumn(M7.postalCodePostalCode,
        regexp_replace(regexp_extract(col(M7.postalCodePostalCode),raw"^(\d+)",1),"^0*", ""))
*/
    /*
        Nota: Hacemos el dropDuplicates para pasar de:

          country_code|postal_code|admin_code1|
          ------------|-----------|-----------|
          AT          |2852       |03         |
          AT          |2852       |01         |
          AT          |2852       |06         |

          Luego de aplicar el dropDuplicates tenemos

          country_code|postal_code|admin_code1|
          ------------|-----------|-----------|
          AT          |2852       |03         |
     */

    postalCodeDF.orderBy(M7.postalCodePostalCode, M7.countryCodePostalCode, M7.regionCodePostalCode).dropDuplicates(Seq(M7.postalCodePostalCode, M7.countryCodePostalCode))


  }

}
