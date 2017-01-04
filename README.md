# navigator-partner
Examples on how to use Cloudera Navigator REST and Java SDK APIs

Instructions on how to run this project.  

1. Make sure Maven is installed on your machine and on your path
2. mvn clean --> to start fresh
3. mvn install --> create the navigator-partner-1.0.jar file
4. Run the Java classes (HiveMetadataExtraction and HdfsMetadataExtraction).  The first parameter to the Java classes is the configuration file which is found in the src/main/resources directory (sample.conf). Modify the sample.conf to include the IP for the cluster that has Navigator running on it. 

Note: the Java classes have the REST APIs are mentioned as a comment for the corresponding Java SDK functions.
