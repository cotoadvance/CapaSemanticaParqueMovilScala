package local.bigdata.tchile.capa_semantica_parque_movil

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcesoDiario {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------------------------------
    // INICIO PARAMETRIZACION
    // ---------------------------------------------------------------

    // BASE

    val conf = new SparkConf().setMaster("yarn").setAppName("B2B").set("spark.driver.allowMultipleContexts", "true").set("spark.yarn.queue", "default").set("spark.sql.crossJoin.enabled", "true").set("spark.sql.debug.maxToStringFields", "1000").set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml").config("spark.submit.deployMode", "cluster").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // PARAMETROS

    val currentDate = java.time.LocalDateTime.now
    val processYear = currentDate.getYear.toString
    val processMonth = currentDate.getMonthValue.toString.reverse.padTo(2, '0').reverse
    val processDay = currentDate.getDayOfMonth.toString.reverse.padTo(2, '0').reverse

    // ACCESO EXADATA

    val exaDriver = "oracle.jdbc.OracleDriver"
    val exaUrl = "jdbc:oracle:thin:@(DESCRIPTION =(ENABLE=BROKEN)(ADDRESS=(PROTOCOL=TCP)(HOST=smt-scan.tchile.local)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=EXPLOTA)))"
    val exaUser = args(0)
    val exaPass = args(1)

    // DESTINO

    val table = "B2B_SEM_AGR_PARQUE_MOVIL"
    val exadataSchema = "CAPSEM"
    val hiveSchema = "visualizacion"

    // PATHS

    val conformadoBasePath = "/modelos/visualizacion/agrupado/b2b_sem_agr_parque_movil/conformado"
    val conformadoFullPath = s"$conformadoBasePath/year=$processYear/month=$processMonth/day=$processDay"
    val parqueMovilColivingPath = "/data/producto_asignado/parque_movil/control_de_gestion/parque_movil_coliving/raw"

    // ---------------------------------------------------------------
    // FIN PARAMETRIZACION
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO PROCESO
    // ---------------------------------------------------------------

    val conformado = spark.read.parquet(parqueMovilColivingPath).
      withColumn("CUSTOMER_SUB_TYPE_DESC", trim($"CUSTOMER_SUB_TYPE_DESC")).
      withColumn(
        "SEGMENTO",
        when($"CUSTOMER_SUB_TYPE_DESC" === "Mediana Empresa", "Mediana").
          when($"CUSTOMER_SUB_TYPE_DESC" === "Pequeña Empresa", "Pequeña").
          otherwise($"CUSTOMER_SUB_TYPE_DESC")
      ).
      filter(
        trim($"PAYMENT_CATEGORY_DESC").isin("Postpaid Payment", "Hybrid Payment")
        and $"CUSTOMER_SUB_TYPE_DESC".isin("Mediana Empresa", "Micro", "Pequeña Empresa", "Corporaciones", "Gran Empresa")
        and $"VALID_IND" === 1
        and date_format($"CLOSE_DATE", "yyyy") >= 2021
      ).
      groupBy("CLOSE_DATE", "SEGMENTO").
      agg(countDistinct("PRIMARY_RESOURCE_VALUE").as("CANT_LINEAS"), countDistinct("RUT_ID").as("CANT_RUT")).
      withColumn("AGNO", date_format(to_date($"CLOSE_DATE"), "yyyy")).
      withColumn("MES_", date_format(to_date($"CLOSE_DATE"), "MMM")).
      withColumn(
        "MES",
        when($"MES_" === "Jan", "Enero").
          when($"MES_" === "Feb", "Febrero").
          when($"MES_" === "Mar", "Marzo").
          when($"MES_" === "Apr", "Abril").
          when($"MES_" === "May", "Mayo").
          when($"MES_" === "Jun", "Junio").
          when($"MES_" === "Jul", "Julio").
          when($"MES_" === "Aug", "Agosto").
          when($"MES_" === "Sep", "Septiembre").
          when($"MES_" === "Oct", "Octubre").
          when($"MES_" === "Nov", "Noviembre").
          when($"MES_" === "Dec", "Diciembre")
      ).
      withColumn("FECHA_CARGA", current_date()).
      withColumn("TIPO_CONTRATO", lit("MOVIL")).
      select(
        "AGNO",
        "MES",
        "SEGMENTO",
        "CANT_LINEAS",
        "CANT_RUT",
        "FECHA_CARGA",
        "TIPO_CONTRATO"
      )

    // ---------------------------------------------------------------
    // FIN PROCESO
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA HDFS
    // ---------------------------------------------------------------

    conformado.repartition(1).write.mode("overwrite").parquet(conformadoFullPath)

    // ---------------------------------------------------------------
    // FIN ESCRITURA HDFS
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA HIVE
    // ---------------------------------------------------------------

    spark.sql(s"DROP TABLE IF EXISTS $hiveSchema.$table")

    spark.sql(
      s"""CREATE EXTERNAL TABLE $hiveSchema.$table(
         |AGNO string,
         |MES string,
         |SEGMENTO string,
         |CANT_LINEAS long,
         |CANT_RUT long,
         |FECHA_CARGA date,
         |TIPO_CONTRATO string)
         |PARTITIONED BY
         |(year string, month string, day string)
         |STORED AS PARQUET
         |LOCATION '$conformadoBasePath'""".stripMargin)

    spark.sql(s"MSCK REPAIR TABLE $hiveSchema.$table")

    // ---------------------------------------------------------------
    // FIN ESCRITURA HIVE
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA EXADATA
    // ---------------------------------------------------------------

    val doTheTruncate = s"CALL $exadataSchema.DO_THE_TRUNCATE('$table')"

    spark.read.format("jdbc").
      option("url", exaUrl).
      option("driver", exaDriver).
      option("sessionInitStatement", doTheTruncate).
      option("dbtable", s"$exadataSchema.$table").
      option("user", exaUser).
      option("password", exaPass).
      option("numPartitions", 1).
      load().show(1)

    conformado.write.format("jdbc").
      option("numPartitions", 2).
      option("url", exaUrl).
      option("dbtable", s"$exadataSchema.$table").
      option("user", exaUser).
      option("password", exaPass).
      option("driver", exaDriver).
      mode("append").save()

    // ---------------------------------------------------------------
    // END ESCRITURA EXADATA
    // ---------------------------------------------------------------

    spark.close()
  }
}