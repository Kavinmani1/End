package com.endstream.practice
import com.endstream.practice.test.logger
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
object Nppes {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("File Streaming Demo")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.sql.shuffle.paritions", 3)
      .config("stopGracefullyOnShutdown", true)
      .getOrCreate()


    val rawDF = spark.read.option("header", true)
      .format("csv")
      .option("path", "input")
      .option("maxFilesPerTrigger", 1)
      .load()

    val deactiveDF = spark.read.option("header", true)
      .format("csv")
      .option("path", "DeactivationInput")
      .option("maxFilesPerTrigger", 1)
      .load()

    val dropfieldDF = rawDF.drop("Replacement NPI", "Healthcare Provider Primary Taxonomy Switch_11", "Provider License Number State Code_11", "Healthcare Provider Taxonomy Code_15", "Other Provider Identifier Type Code_19", "Other Provider Identifier Type Code_22", "Other Provider Identifier_33",
      "Other Provider Identifier Issuer_36", "Other Provider Identifier_42", "Other Provider Identifier_40", "Other Provider Identifier Issuer_46")


    //val newdeactivation = deactiveDF.withColumnRenamed("NPI", "NPID")
    //dropfieldDF.createOrReplaceTempView("dropfieldDF")
    ///newdeactivation.createOrReplaceTempView("DF")
    //val join1 = dropfieldDF.join(deactiveDF, dropfieldDF("NPI") === deactiveDF("NPI"), "left").where(deactiveDF("NPI") isNull).show(false)
    //val join2 = dropfieldDF.join(deactiveDF, dropfieldDF("NPI") === deactiveDF("NPI"), "inner").show(false)


    import spark.sqlContext.implicits._

   // val column=Seq("NPI","Entity Type Code","Employer Identification Number (EIN)","Provider Organization Name (Legal Business Name)","Provider Last Name (Legal Name)","Provider First Name","Provider Middle Name","Provider Name Prefix Text","Provider Name Suffix Text","Provider Credential Text","Provider Other Organization Name","Provider Other Organization Name Type Code","Provider Other Last Name","Provider Other First Name","Provider Other Middle Name","Provider Other Name Prefix Text","Provider Other Name Suffix Text","Provider Other Credential Text","Provider Other Last Name Type Code","Provider First Line Business Mailing Address","Provider Second Line Business Mailing Address","Provider Business Mailing Address City Name","Provider Business Mailing Address State Name","Provider Business Mailing Address Postal Code","Provider Business Mailing Address Country Code (If outside U.S.)","Provider Business Mailing Address Telephone Number","Provider Business Mailing Address Fax Number","Provider First Line Business Practice Location Address","Provider Second Line Business Practice Location Address","Provider Business Practice Location Address City Name","Provider Business Practice Location Address State Name","Provider Business Practice Location Address Postal Code","Provider Business Practice Location Address Country Code (If outside U.S.)","Provider Business Practice Location Address Telephone Number","Provider Business Practice Location Address Fax Number","Provider Enumeration Date","Last Update Date","NPI Deactivation Reason Code","NPI Deactivation Date","NPI Reactivation Date","Provider Gender Code","Authorized Official Last Name","Authorized Official First Name","Authorized Official Middle Name","Authorized Official Title or Position","Authorized Official Telephone Number","Healthcare Provider Taxonomy Code_1","Provider License Number_1","Provider License Number State Code_1","Healthcare Provider Primary Taxonomy Switch_1","Healthcare Provider Taxonomy Code_2","Provider License Number_2","Provider License Number State Code_2","Healthcare Provider Primary Taxonomy Switch_2","Healthcare Provider Taxonomy Code_3","Provider License Number_3","Provider License Number State Code_3","Healthcare Provider Primary Taxonomy Switch_3","Healthcare Provider Taxonomy Code_4","Provider License Number_4","Provider License Number State Code_4","Healthcare Provider Primary Taxonomy Switch_4","Healthcare Provider Taxonomy Code_5","Provider License Number_5","Provider License Number State Code_5","Healthcare Provider Primary Taxonomy Switch_5","Healthcare Provider Taxonomy Code_6","Provider License Number_6","Provider License Number State Code_6","Healthcare Provider Primary Taxonomy Switch_6","Healthcare Provider Taxonomy Code_7","Provider License Number_7","Provider License Number State Code_7","Healthcare Provider Primary Taxonomy Switch_7","Healthcare Provider Taxonomy Code_8","Provider License Number_8","Provider License Number State Code_8","Healthcare Provider Primary Taxonomy Switch_8","Healthcare Provider Taxonomy Code_9","Provider License Number_9","Provider License Number State Code_9","Healthcare Provider Primary Taxonomy Switch_9","Healthcare Provider Taxonomy Code_10","Provider License Number_10","Provider License Number State Code_10","Healthcare Provider Primary Taxonomy Switch_10","Healthcare Provider Taxonomy Code_11","Provider License Number_11","Healthcare Provider Taxonomy Code_12","Provider License Number_12","Provider License Number State Code_12","Healthcare Provider Primary Taxonomy Switch_12","Healthcare Provider Taxonomy Code_13","Provider License Number_13","Provider License Number State Code_13","Healthcare Provider Primary Taxonomy Switch_13","Healthcare Provider Taxonomy Code_14","Provider License Number_14","Provider License Number State Code_14","Healthcare Provider Primary Taxonomy Switch_14","Provider License Number_15","Provider License Number State Code_15","Healthcare Provider Primary Taxonomy Switch_15","Other Provider Identifier_1","Other Provider Identifier Type Code_1","Other Provider Identifier State_1","Other Provider Identifier Issuer_1","Other Provider Identifier_2","Other Provider Identifier Type Code_2","Other Provider Identifier State_2","Other Provider Identifier Issuer_2","Other Provider Identifier_3","Other Provider Identifier Type Code_3","Other Provider Identifier State_3","Other Provider Identifier Issuer_3","Other Provider Identifier_4","Other Provider Identifier Type Code_4","Other Provider Identifier State_4","Other Provider Identifier Issuer_4","Other Provider Identifier_5","Other Provider Identifier Type Code_5","Other Provider Identifier State_5","Other Provider Identifier Issuer_5","Other Provider Identifier_6","Other Provider Identifier Type Code_6","Other Provider Identifier State_6","Other Provider Identifier Issuer_6","Other Provider Identifier_7","Other Provider Identifier Type Code_7","Other Provider Identifier State_7","Other Provider Identifier Issuer_7","Other Provider Identifier_8","Other Provider Identifier Type Code_8","Other Provider Identifier State_8","Other Provider Identifier Issuer_8","Other Provider Identifier_9","Other Provider Identifier Type Code_9","Other Provider Identifier State_9","Other Provider Identifier Issuer_9","Other Provider Identifier_10","Other Provider Identifier Type Code_10","Other Provider Identifier State_10","Other Provider Identifier Issuer_10","Other Provider Identifier_11","Other Provider Identifier Type Code_11","Other Provider Identifier State_11","Other Provider Identifier Issuer_11","Other Provider Identifier_12","Other Provider Identifier Type Code_12","Other Provider Identifier State_12","Other Provider Identifier Issuer_12","Other Provider Identifier_13","Other Provider Identifier Type Code_13","Other Provider Identifier State_13","Other Provider Identifier Issuer_13","Other Provider Identifier_14","Other Provider Identifier Type Code_14","Other Provider Identifier State_14","Other Provider Identifier Issuer_14","Other Provider Identifier_15","Other Provider Identifier Type Code_15","Other Provider Identifier State_15","Other Provider Identifier Issuer_15","Other Provider Identifier_16","Other Provider Identifier Type Code_16","Other Provider Identifier State_16","Other Provider Identifier Issuer_16","Other Provider Identifier_17","Other Provider Identifier Type Code_17","Other Provider Identifier State_17","Other Provider Identifier Issuer_17","Other Provider Identifier_18","Other Provider Identifier Type Code_18","Other Provider Identifier State_18","Other Provider Identifier Issuer_18","Other Provider Identifier_19","Other Provider Identifier State_19","Other Provider Identifier Issuer_19","Other Provider Identifier_20","Other Provider Identifier Type Code_20","Other Provider Identifier State_20","Other Provider Identifier Issuer_20","Other Provider Identifier_21","Other Provider Identifier Type Code_21","Other Provider Identifier State_21","Other Provider Identifier Issuer_21","Other Provider Identifier_22","Other Provider Identifier State_22","Other Provider Identifier Issuer_22","Other Provider Identifier_23","Other Provider Identifier Type Code_23","Other Provider Identifier State_23","Other Provider Identifier Issuer_23","Other Provider Identifier_24","Other Provider Identifier Type Code_24","Other Provider Identifier State_24","Other Provider Identifier Issuer_24","Other Provider Identifier_25","Other Provider Identifier Type Code_25","Other Provider Identifier State_25","Other Provider Identifier Issuer_25","Other Provider Identifier_26","Other Provider Identifier Type Code_26","Other Provider Identifier State_26","Other Provider Identifier Issuer_26","Other Provider Identifier_27","Other Provider Identifier Type Code_27","Other Provider Identifier State_27","Other Provider Identifier Issuer_27","Other Provider Identifier_28","Other Provider Identifier Type Code_28","Other Provider Identifier State_28","Other Provider Identifier Issuer_28","Other Provider Identifier_29","Other Provider Identifier Type Code_29","Other Provider Identifier State_29","Other Provider Identifier Issuer_29","Other Provider Identifier_30","Other Provider Identifier Type Code_30","Other Provider Identifier State_30","Other Provider Identifier Issuer_30","Other Provider Identifier_31","Other Provider Identifier Type Code_31","Other Provider Identifier State_31","Other Provider Identifier Issuer_31","Other Provider Identifier_32","Other Provider Identifier Type Code_32","Other Provider Identifier State_32","Other Provider Identifier Issuer_32","Other Provider Identifier Type Code_33","Other Provider Identifier State_33","Other Provider Identifier Issuer_33","Other Provider Identifier_34","Other Provider Identifier Type Code_34","Other Provider Identifier State_34","Other Provider Identifier Issuer_34","Other Provider Identifier_35","Other Provider Identifier Type Code_35","Other Provider Identifier State_35","Other Provider Identifier Issuer_35","Other Provider Identifier_36","Other Provider Identifier Type Code_36","Other Provider Identifier State_36","Other Provider Identifier_37","Other Provider Identifier Type Code_37","Other Provider Identifier State_37","Other Provider Identifier Issuer_37","Other Provider Identifier_38","Other Provider Identifier Type Code_38","Other Provider Identifier State_38","Other Provider Identifier Issuer_38","Other Provider Identifier_39","Other Provider Identifier Type Code_39","Other Provider Identifier State_39","Other Provider Identifier Issuer_39","Other Provider Identifier Type Code_40","Other Provider Identifier State_40","Other Provider Identifier Issuer_40","Other Provider Identifier_41","Other Provider Identifier Type Code_41","Other Provider Identifier State_41","Other Provider Identifier Issuer_41","Other Provider Identifier Type Code_42","Other Provider Identifier State_42","Other Provider Identifier Issuer_42","Other Provider Identifier_43","Other Provider Identifier Type Code_43","Other Provider Identifier State_43","Other Provider Identifier Issuer_43","Other Provider Identifier_44","Other Provider Identifier Type Code_44","Other Provider Identifier State_44","Other Provider Identifier Issuer_44","Other Provider Identifier_45","Other Provider Identifier Type Code_45","Other Provider Identifier State_45","Other Provider Identifier Issuer_45","Other Provider Identifier_46","Other Provider Identifier Type Code_46","Other Provider Identifier State_46","Other Provider Identifier_47","Other Provider Identifier Type Code_47","Other Provider Identifier State_47","Other Provider Identifier Issuer_47","Other Provider Identifier_48","Other Provider Identifier Type Code_48","Other Provider Identifier State_48","Other Provider Identifier Issuer_48","Other Provider Identifier_49","Other Provider Identifier Type Code_49","Other Provider Identifier State_49","Other Provider Identifier Issuer_49","Other Provider Identifier_50","Other Provider Identifier Type Code_50","Other Provider Identifier State_50","Other Provider Identifier Issuer_50","Is Sole Proprietor","Is Organization Subpart","Parent Organization LBN","Parent Organization TIN","Authorized Official Name Prefix Text","Authorized Official Name Suffix Text","Authorized Official Credential Text","Healthcare Provider Taxonomy Group_1","Healthcare Provider Taxonomy Group_2","Healthcare Provider Taxonomy Group_3","Healthcare Provider Taxonomy Group_4","Healthcare Provider Taxonomy Group_5","Healthcare Provider Taxonomy Group_6","Healthcare Provider Taxonomy Group_7","Healthcare Provider Taxonomy Group_8","Healthcare Provider Taxonomy Group_9","Healthcare Provider Taxonomy Group_10","Healthcare Provider Taxonomy Group_11","Healthcare Provider Taxonomy Group_12","Healthcare Provider Taxonomy Group_13","Healthcare Provider Taxonomy Group_14","Healthcare Provider Taxonomy Group_15","Certification Date")


    val gender  = dropfieldDF.withColumn("Provider Gender Code", when(col("Provider Gender Code") === "M", "M,Male")
      .when(col("Provider Gender Code") === "F", "F,Female"))


   // val EndpointType = gender.withColumn(" Endpoint Type", when(col("Endpoint Type") === "SOAP", "SOAP, SOAP WS URL")
    //  .when(col("Endpoint Type") === "CONNECT", " CONNECT,Connect URL").when(col("Endpoint Type") === "FHIR", "FHIR,FHIR URL").when(col("Endpoint Type") === "DIRECT", "DIRECT, Direct Address").when(col("Endpoint Type") === "REST", " REST,Restful WS URL").when(col("Endpoint Type") === "WEB", "WEB, Website URL").when(col("Endpoint Type") === "OTHERS", " OTHERS,Other URL"))

    //val GroupTaxonomy = gender.withColumn("Group Taxonomy Code", when(col("Group Taxonomy Code") === "193200000X", " 193200000X, Multi-Specialty Group")
     // .when(col("Group Taxonomy Code") === "193400000X", "193400000X, Single Specialty Group"))



    //val PrimaryTaxonomy = gender.withColumn("Primary Taxonomy Switch_1", when(col("Primary Taxonomy Switch_1") === "X", " X, Not Answered")
      //.when(col("Primary Taxonomy Switch_1") === "Y", "Y, Yes").when(col("Primary Taxonomy Switch_1") === "N", "N, No"))



   // val OtherProviderTypeCode = gender.withColumn(" Other Provider Identifier Type Code_1", when(col(" Other Provider Identifier Type Code_1") === "01", " 01, OTHER")
     // .when(col(" Other Provider Identifier Type Code_1") === "05", "05, MEDICAID"))

    val df3 = gender.withColumn("Entity Type Code", when(col("Entity Type Code") === "1", "1, Individual")
      .when(col("Entity Type Code") === "2", "2, Organization"))

    val df4 = df3.withColumn("Is Sole Proprietor", when(col("Is Sole Proprietor") === "X", "X, Not Answered")
      .when(col("Is Sole Proprietor") === "Y", "Y, Yes")
      .when(col("Is Sole Proprietor") === "N", "N, No"))

    val df5 = df4.withColumn("Is Organization Subpart", when(col("Is Organization Subpart") === "X", "X, Not Answered")
      .when(col("Is Organization Subpart") === "Y", "Y, Yes")
      .when(col("Is Organization Subpart") === "N", "N, No"))

    val df6 = df5.withColumn("Provider Other Organization Name Type Code", when(col("Provider Other Organization Name Type Code") === "1", "1, Former Name, I")
      .when(col("Provider Other Organization Name Type Code") === "2", "2, Professional Name, I")
      .when(col("Provider Other Organization Name Type Code") === "3", "3, Doing Business As, O")
      .when(col("Provider Other Organization Name Type Code") === "4", "4, Former Legal Business Name, O")
      .when(col("Provider Other Organization Name Type Code") === "5", "5, Other Name, B"))

    val df7 = df6.withColumn("Provider Other Last Name Type Code", when(col("Provider Other Last Name Type Code") === "1", "1, Former Name, I")
      .when(col("Provider Other Last Name Type Code") === "2", "2, Professional Name, I")
      .when(col("Provider Other Last Name Type Code") === "3", "3, Doing Business As, O")
      .when(col("Provider Other Last Name Type Code") === "4", "4, Former Legal Business Name, O")
      .when(col("Provider Other Last Name Type Code") === "5", "5, Other Name, B"))

    val df8 = df7.withColumn("Other Provider Identifier Type Code_10", when(col("Other Provider Identifier Type Code_10") === "01", "01, OTHER")
      .when(col("Other Provider Identifier Type Code_10") === "05", "05, MEDICAID")
    )

    df8.show(false)

    val npiColumn = df8.select(Seq.empty[org.apache.spark.sql.Column] ++
      df8.columns.map(colName => col("`" + colName + "`").as(colName.toLowerCase.replace(" ", "_"))): _*)


    val finaldeactivation = npiColumn.withColumnRenamed("npi", "npid")

    val join1 = finaldeactivation.join(deactiveDF, finaldeactivation("npid") === deactiveDF("npi"), "left").where(deactiveDF("npi") isNull)
    val join2 = finaldeactivation.join(deactiveDF, finaldeactivation("npid") === deactiveDF("npi"), "inner")


    val activated = join1.select(col("npid"), col("entity_type_code"),col("provider_business_mailing_address_city_name"),col("provider_business_mailing_address_postal_code"),col("provider_business_mailing_address_telephone_number")
      ,col("is_sole_proprietor"),col("provider_business_mailing_address_fax_number"),
      col("provider_other_last_name_type_code"), col("provider_gender_code"))
    activated.printSchema()
    activated.show()

    val deactivated = join2.select(col("npi"), col("entity_type_code"),col("provider_business_mailing_address_city_name"), col("provider_business_mailing_address_postal_code"), col("provider_business_mailing_address_telephone_number")
      , col("is_sole_proprietor"), col("provider_business_mailing_address_fax_number"),
      col("provider_other_last_name_type_code"), col("provider_gender_code"))
    deactivated.printSchema()
    deactivated.show()

    activated.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "finalpoc")
      .option("table", "active_members")
      .mode("append")
      .save()

    activated.show()


    deactivated.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "finalpoc")
      .option("table", "deactive_members")
      .mode("append")
      .save()

    deactivated.show()




  /*  uactivated2.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "active", "keyspace" -> "finalpoc"))
      .mode("append")
      .save()

    udeactivated2.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "deactivate_table", "keyspace" -> "finalpoc"))
      .mode("append")
      .save()*/


    // val uactivated= activatednew.withColumn("npi", col("npi").cast(StringType))

   /* join1.selectExpr("cast(npi.npi as String) as  key", """to_json(struct(*))as value""".stripMargin)
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "topic1")
      .save()*/

    //print(f"Row written to topic {topic1}")


    /*val df = rawDF.toDF(column: _*)
    val Gendercode = Map(("M", "Male"), ("F", "Female"))
    val broadcastGendercode = spark.sparkContext.broadcast(Gendercode)
    val df2 = df.map(f => {
      val gendercode =f.getString(41)
      val fullgender = broadcastGendercode.value.get(gendercode).get
      (getString(41),fullgender)
    })
    df2.show()
    df.show(false)*/



    /*val smallerDF = Seq(("C", "Celcius"),
      ("F", "Fahrenheit")
    ).toDF("code", "realUnit")*/


    /*val DeactiveWriterQuery = joinDF2.repartition(1).write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "activated")
      .save()*/


    /* val deactivationdf = dropfieldDF.col("NPI") === deactiveDF.col("NPI")
    val joinType = "inner"
    val actiavationDF= dropfieldDF.col("NPI") =!= deactiveDF.col("NPI")
    val newDF1= dropfieldDF.join(deactiveDF,actiavationDF,joinType)
    val newDF = dropfieldDF.join(deactiveDF, deactivationdf, joinType)


    val DeactiveWriterQuery = newDF.repartition(1).writeStream
      .format("csv")
      .queryName("Flattened Deactive Writer")
      .outputMode("append")
      .option("path", "DeactiveOutput")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    val activeWriterQuery = newDF1.repartition(1).writeStream
      .format("csv")
      .queryName("Flattened active Writer ")
      .outputMode("append")
      .option("path", "ActivationOutput")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Flattened active Writer started")
    activeWriterQuery.awaitTermination()

    logger.info("Flattened deactive Writer started")
    DeactiveWriterQuery.awaitTermination()*/


    /*val notificationWriterQuery = kafka
      .writeStreamTargetDF
      .queryName("Notification Writer")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "notifications")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir")
      .start()

    logger.info("Flattened Invoice Writer started")
    invoiceWriterQuery.awaitTermination()*/


    /* val npiDataColumn = dropfieldDF.select(Seq.empty[org.apache.spark.sql.Column] ++
         dropfieldDF.columns.map(colName => col("`" + colName + "`").as(colName.toLowerCase.replace(" ", "_")))
         : _*)
       val daColumn = deactiveDF.select(Seq.empty[org.apache.spark.sql.Column] ++
         deactiveDF.columns.map(colName => col("`" + colName + "`").as(colName.toLowerCase.replace(" ", "_")))
         : _*)*/

    /*val joinDF3 = spark.sql("select * from dropfieldDF join DF on dropfieldDF.NPI == DF.NPID ")
        joinDF3.show()
        val joinDF2 = spark.sql("select * from dropfieldDF Left join DF on dropfieldDF.NPI == DF.NPID where DF.NPID is null")
        joinDF2.show()*/


    /*val spark = SparkSession.builder()
      .master("local[3]")
      .appName("File Streaming Demo")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()*/

  }
}


