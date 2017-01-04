/*
 * Copyright (c) 2016 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.navigator.navigator_partner.extraction;

import com.cloudera.nav.sdk.client.MetadataExtractor;
import com.cloudera.nav.sdk.client.MetadataResultSet;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.QueryUtils;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.custom.CustomProperty;
import com.cloudera.nav.sdk.model.custom.CustomPropertyType;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.entities.HdfsEntity;
import com.cloudera.nav.sdk.model.entities.HiveTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This is a sample program that runs different metadata extractions and 
 * updates on HDFS entities. It shows how to create tags and custom properties.
 *
 * Program arguments:
 * 1. path to config file: see examples/src/main/resources/sample.conf
 * 2. output path: where to write the extracted marker for next run
 *
 */
public class HdfsMetadataExtraction {

  public static void main(String[] args) throws IOException {
    // handle arguments
    Preconditions.checkArgument(args.length >= 1);
    String configFilePath = args[0];
    String marker = null;

    NavigatorPlugin navPlugin = NavigatorPlugin.fromConfigFile(configFilePath);
    NavApiCient client = navPlugin.getClient();
   
    MetadataExtractor extractor = new MetadataExtractor(client, null);

    // Run filtered examples
    getHDFSEntities(client, extractor, marker);
    addCustomTags(navPlugin, extractor, marker); 
    
  }

  /**
   * How to retrieve various HDFS entities. 
   * Comments describe how to do the equivalent functions using the REST APIs. 
   * 
   * @param NavApiCient - Navigator client used to communicate with Navigator service
   * @param extractor - used to extract the data
   * @param marker - is set to null, but is a string cursor return by the server when extraction occurs
   */
  public static void getHDFSEntities(NavApiCient client,
                                     MetadataExtractor extractor,
                                     String marker) {
	  
	//retrieve all HDFS source types
	//REST API equivalent
	//curl 'http://fkader-nav-1.vpc.cloudera.com:7187/api/v9/entities?query=((sourceType:HDFS))' -u admin:admin -X GET
    Iterable<Map<String, Object>> HdfsAll =
        extractor.extractMetadata(marker, null, "sourceType:HDFS", null)
            .getEntities();
    getFirstResult(HdfsAll);

    //retrieve by SourceId (unique entity)
    //REST API equivalent
    // curl 'http://fkader-nav-1.vpc.cloudera.com:7187/api/v9/entities?query=((sourceType:HDFS)AND(sourceId:xxxxx)' -u admin:admin -X GET
    Source hdfsSource = client.getOnlySource(SourceType.HDFS);
    Iterable<Map<String, Object>> HdfsSingleSource =
        extractor.extractMetadata(marker, null,  "sourceType:HDFS AND " +
            "sourceId:" + hdfsSource.getIdentity(), null).getEntities();
    getFirstResult(HdfsSingleSource);
    
    
    //retrieve only sample_07 files in HDFS 
    //REST API equivalent
    //curl 'http://fkader-nav-1.vpc.cloudera.com:7187/api/v9/entities?query=((type:FILE)AND(sourceType:HDFS)AND(originalName:sample_07))' -u admin:admin -X GET
    Iterable<Map<String, Object>> hdfsFile =
        extractor.extractMetadata(marker, null, "sourceType:HDFS AND type:FILE AND originalName:sample_07", null).getEntities();
    getFirstResult(hdfsFile);
    
    //retrieve all directories in HDFS
    //REST API equivalent
    //curl 'http://fkader-nav-1.vpc.cloudera.com:7187/api/v9/entities?query=((type:DIRECTORY)AND(sourceType:HDFS))' -u admin:admin -X GET
    Iterable<Map<String, Object>> hdfsDir =
            extractor.extractMetadata(marker, null, "sourceType:HDFS AND type:DIRECTORY", null).getEntities();
    getFirstResult(hdfsDir);
    
    
  } 
  
  /**
   * Create custom metadata as tags and key:value properties for HDFS FILE (sample_08). 
   * Comments describe how to do the equivalent functions using the REST APIs. 
   * 
   * @param navPlugin - Navigator Plugin to communicate with Navigator service
   * @param extractor - used to extract the data
   * @param marker - is set to null, but is a string cursor return by the server when extraction occurs
   */
  public static void addCustomTags (NavigatorPlugin navPlugin, 
		  MetadataExtractor extractor, String marker) {
	  

	  //REST API  
	  //curl 'http://fkader-nav-1.vpc.cloudera.com:7187/api/v9/entities?query=((type:FILE)AND(sourceType:HDFS)AND(originalName:sample_08)AND(type:DIRECTORY))' -u admin:admin -X GET

	  StringBuffer querybuff = new StringBuffer();
	  querybuff.append("sourceType:HDFS ") 
	  //AND sourceId:a09b0233cc58ff7d601eaa68673a20c6")
	  	.append(" AND type:FILE ")
	  	.append("AND originalName:sample_07");
	  
	  //get the sourceId
	  Iterable<Map<String, Object>> hdfsDir = 
			  extractor.extractMetadata(marker, null, querybuff.toString(), null).getEntities();
			 
	  //add tags using the REST API:
	  //curl http://fkader-nav-1.vpc.cloudera.com:7187/api/v9/entities/ -u admin:admin -X POST -H "Content-Type: application/json" -d '{"sourceId":"a09b0233cc58ff7d601eaa68673a20c6","parentPath":"/user/admin","originalName":"sample_09","name":"navtest_custom_props","description":"Navigator Custom Properties", "properties":{"creator":"partnerCreated"}, "tags":["tag1", "tag2"]}'

	  Iterator<Map<String, Object>>  iterHdfsDir = hdfsDir.iterator();
	  if (iterHdfsDir.hasNext()) {
		  Map<String, Object> result = iterHdfsDir.next();
		  
		  HdfsEntity modifiedSampleDir = new HdfsEntity();
		  
	      modifiedSampleDir.setSourceId(result.get("sourceId").toString());
	      modifiedSampleDir.setEntityType(EntityType.FILE);
	      modifiedSampleDir.setFileSystemPath(result.get("fileSystemPath").toString());
	      modifiedSampleDir.setName(result.get("originalName").toString());
	      modifiedSampleDir.setDescription("Navigator Custom Properties");	      
	      modifiedSampleDir.addTags("tag1");
	      modifiedSampleDir.addTags("tag2");

	      
	      //key:value properties tag
	      Map<String, String> props = new HashMap<String, String>();
	      props.put("creator", "testpartnerCreated");
	      modifiedSampleDir.addProperties(props); 
	      	      
	      //create new namespace 
	   //   modifiedSampleDir.setNamespace("partnernamespace");

	      modifiedSampleDir.setProperties(props);
	      
	      ResultSet results = navPlugin.write(modifiedSampleDir);
		  if (results.hasErrors()) {
	        throw new RuntimeException(results.toString());
	      }
		  System.out.println("successfully updated sample hdfs directory" );
	         
	  }	  
  }
 
  private static void getFirstResult(Iterable<Map<String, Object>> iterable) {
    // In real usage, iterate through results and process each metadata object
    Iterator<Map<String, Object>> iterator = iterable.iterator();
    if(iterator.hasNext()) {
      Map<String, Object> result = iterator.next();
      System.out.println("source: " + result.get("sourceType") +
          "  type: " + result.get("type") + " originalName " +  result.get("originalName")
          + " file location " + result.get("fileSystemPath")
          + " parent path " + result.get("parentPath")
    		  );
    } else {
      System.out.println("no elements found");
    }
  } 
  
}
