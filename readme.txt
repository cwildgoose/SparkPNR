This Maven project represents an approach for pulling data out of an example avro file that is representative of
a similar file that will reside in the hadoop data lake. The file has one minor modification from the original file,
the type of the body field was changed from bytes to string. This was made because the spark-avro parser was not able
to parse this field. 
In order to use this Maven Project:

1. Install JDK 1.7
2. Install Scala IDE from the Eclipse MarketPlace.
3. Import the existing maven Project into Eclipse.
4. Ensure that PNR.xml, and PNR.parquet folders are deleted before running.
4. Choose PNRAgent.scala and Run AS SCala Project to run the project.
5. After running, the Directory PNR.parquet should be created.
6. Copy the folder to a directory in HADOOP.
7. Go into hive and create a table that points to the HADOOP the location of the files. Run createPNR_AGENT.hive.
8. Type select * from TEST.PNR_AGENT and view the data.
