package com.sabre.bigdata.connectors

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.avro.mapred.{AvroOutputFormat, FsInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.avro.file.{DataFileConstants, DataFileReader}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,StructField,StringType}


import java.io.{File,FileNotFoundException}

object PNRagent {
  private var pnrFile:String = "data.PNR.avro"
  private var sqlContext: SQLContext = _
  private var sc:SparkConf = new SparkConf()
  
  def main(args: Array[String]):Unit = {
    
   val conf = sc.setAppName("PNR Connector").setMaster("local[2]")
   val sparkContext = new SparkContext(conf)
   
   sqlContext = new SQLContext(sparkContext)
   sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
   
   //val schema = StructType(Array(StructField("source",StringType,false),StructField("entity",StringType,false),StructField("sourceCustomerID",StringType,true),StructField("sourceCustomerAppID",StringType,true),StructField("sourceTimestamp",StringType,true),StructField("sourceConversationID",StringType,true),StructField("sourceMessageID",StringType,true),StructField("edaMessageID",StringType,false),StructField("edaIngestTimestamp",StringType,false),StructField("edaLoadTimestamp",StringType,false),StructField("validityIndicator",StringType,true),StructField("body",StringType,false)))
   //val dfPNRAvro = sqlContext.read.format("com.databricks.spark.avro").schema(schema).load(pnrFile)
   val dfPNRAvro = sqlContext.read.format("com.databricks.spark.avro").load(pnrFile)
   //dfPNRAvro.show()
   val outputDir = "PNR.xml"
   // sc.textFile("/my/dir1,/my/paths/part-00[0-5]*
   dfPNRAvro.select("body").rdd.map(row=>row.mkString(" ")).saveAsTextFile(outputDir)
    
   val pnrXML= "data.PNR.xml"
   val dfAgent = sqlContext.read.format("com.databricks.spark.xml")
                                             .option("rootTag","TicketingDocument.Notification")
                                                                 .option("rowTag","Agent").load(pnrXML)
   
   
   //dfAgent.show()
   val pnrParquetDir = "PNR.parquet"
   dfAgent.write.parquet(pnrParquetDir)
   
                             
   
      
  }
}