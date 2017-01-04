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
import com.cloudera.nav.sdk.model.entities.HiveTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This is a sample program that runs different metadata extractions and 
 * updates on Hive entities. It shows how to create tags and custom properties.
 *
 * Program arguments:
 * 1. path to config file: see examples/src/main/resources/sample.conf
 * 2. output path: where to write the extracted marker for next run
 *
 */
public class HiveMetadataExtraction {

  public static void main(String[] args) throws IOException {
    // handle arguments
    Preconditions.checkArgument(args.length >= 2);
    String configFilePath = args[0];
    String markerPath = args[1];
    String marker = args.length > 2 ?
    		HiveMetadataExtraction.readFileArg(args[2]) : null;

    NavigatorPlugin navPlugin = NavigatorPlugin.fromConfigFile(configFilePath);   
    NavApiCient client = navPlugin.getClient();
    
   
    MetadataExtractor extractor = new MetadataExtractor(client, null);
    addCustomTags(navPlugin, extractor, marker); 
    
    // Run filtered examples
    getHive(extractor, marker, "salary");
    
  }

  /**
   * How to retrieve various Hive entities. 
   * Comments describe how to do the equivalent functions using the REST APIs. 
   * 
   * @param NavApiCient - Navigator client used to communicate with Navigator service
   * @param extractor - used to extract the data
   * @param marker - is set to null, but is a string cursor return by the server when extraction occurs
   */
  public static void getHive(MetadataExtractor extractor,
                             String marker, String colName) {
	  
	// REST API:
	//curl 'http://fkader-nav-1.vpc.cloudera.com:7187/api/v9/entities?query=((type:DATABASE)AND(sourceType:HIVE))' -u admin:admin -X GET  
    Iterable<Map<String, Object>> hiveDb = extractor.extractMetadata(marker,
        null, "sourceType:HIVE AND type:DATABASE", null).getEntities();
    getFirstResult(hiveDb);

	// REST API:
	//curl 'http://fkader-nav-1.vpc.cloudera.com:7187/api/v9/entities?query=((type:TABLE)AND(sourceType:HIVE))' -u admin:admin -X GET      
    MetadataResultSet hiveTableExtract = extractor.extractMetadata(marker,
            null, "sourceType:HIVE AND type:TABLE", null);
     
    Iterable<Map<String, Object>> hiveTable = hiveTableExtract.getEntities();
    getFirstResult(hiveTable);
    
	// REST API:
	//curl 'http://fkader-nav-1.vpc.cloudera.com:7187/api/v9/entities?query=((type:VIEW)AND(sourceType:HIVE))' -u admin:admin -X GET      
    Iterable<Map<String, Object>> hiveView = extractor.extractMetadata(marker,
            null, "sourceType:HIVE AND type:VIEW", null).getEntities();
    getFirstResult(hiveView);

	// REST API:
	//curl 'http://fkader-nav-1.vpc.cloudera.com:7187/api/v9/entities?query=((type:FIELD)AND(sourceType:HIVE)AND(originalName:xxcolNamexx))' -u admin:admin -X GET      
    Iterable<Map<String, Object>> hiveColumn = extractor.extractMetadata(marker,
        null, "sourceType:HIVE AND type:FIELD " +
        "AND originalName:" + colName, null).getEntities();
    getFirstResult(hiveColumn);
    
    //get the marker to save for next run (will use this as the starting point)
    String hiveExtractMarker = hiveTableExtract.getMarker();
   
    //run the same query again and will see that no data because no changes since run
    MetadataResultSet hiveTableExtractAgain = extractor.extractMetadata(hiveExtractMarker,
            null, "sourceType:HIVE AND type:TABLE", null);
    
    Iterable<Map<String, Object>> hiveTableAgain = hiveTableExtractAgain.getEntities();
        System.out.println("query the data again - should return no results");
        getFirstResult(hiveTableAgain);
   
  } 
  

  /**
   * Create custom metadata as tags and key:value properties for Hive entries. 
   * Comments describe how to do the equivalent functions using the REST APIs. 
   * 
   * @param navPlugin - Navigator Plugin to communicate with Navigator service
   * @param extractor - used to extract the data
   * @param marker - is set to null, but is a string cursor return by the server when extraction occurs
   */
  public static void addCustomTags (NavigatorPlugin navPlugin, 
		  MetadataExtractor extractor, String marker) {
		  
	  //REST API - see HDFSMetadataExtraction for example.. 
	  //add tags to the cart_items table
	  Iterable<Map<String, Object>> sampleTable = extractor.extractMetadata(marker,
			  null, "sourceType:HIVE AND type:TABLE AND originalName:cart_items", null)
			  .getEntities();
	  
	  Iterator<Map<String, Object>>  iterSampleTable = sampleTable.iterator();
	  
	  if (iterSampleTable.hasNext()) {
		  Map<String, Object> result = iterSampleTable.next();
		  
		  HiveTable modifiedSampleTable = new HiveTable();
	      modifiedSampleTable.setSourceId(result.get("sourceId").toString());
	      modifiedSampleTable.setDatabaseName("default");
	      modifiedSampleTable.setTableName(result.get("originalName").toString());
	      modifiedSampleTable.addTags("sampletabletag1");
	      
	      //key:value properties tag
	      Map<String, String> props = new HashMap<String, String>();
	      props.put("sampleKeyProp", "sampleValueProp");
	      props.put("creator", "test");
	      modifiedSampleTable.addProperties(props);
	      ResultSet results = navPlugin.write(modifiedSampleTable);
		  if (results.hasErrors()) {
	        throw new RuntimeException(results.toString());
	      }
		  System.out.println("successfully updated table" );
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
  
  static String readFileArg(String path) throws IOException {
	  try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
		  return reader.readLine();
		  }
	  }
  
}
