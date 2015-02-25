#What is Tidoop

Tidoop is a suite of extensions for [Hadoop](apache.hadoop.org), the <i>defacto</i> Big Data standard for batch data analysis based in the MapReduce paradigm.

Typically, MapReduce applications within Hadoop read the data to be analyzed from HDFS, the Hadoop Distributed File System. Neverhteless, it may happen the data meant to be analyzed is not in HDFS, but in a remote non-HDFS storage. In that cases, the data must be moved to HDFS in order it can be read by MapReduce applications.

By using Tidoop you will not need to copy data from remote to HDFS anymore, but you will be able to read such data by pointing to it. At least, this is true if your data is stored at:

* [CKAN](ckan.org), the Open Data platform.
* [NGSI Short-Term Historic](https://github.com/telefonicaid/IoT-STH), the NGSI-oriented storage for [Orion Context Broker](https://github.com/telefonicaid/fiware-orion).

The extensions provide specific Java classes per each remote storage type; those classes must be packaged into a Java jar file.
