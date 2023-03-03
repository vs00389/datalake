package com.datalake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;

/**
 * This example shows how to start using the Azure Storage Data Lake SDK for Java.
 */
public class DemoDataLakeJavaTable {

    /**
     * Entry point into the basic examples for Storage datalake.
     *
     * @param args Unused. Arguments to the program.
     * @throws IOException If an I/O error occurs
     * @throws RuntimeException If the downloaded data doesn't match the uploaded data
     */
    public static void main(String[] args) throws IOException {

        /*
         * From the Azure portal, get your Storage account's name and account key.
         */
    	//https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/tables/azure-data-tables/README.md
    	System.out.println("BEGIN__________");
        String accountName = "datalakesa99";
        String accountKey = "7ALgiUpcfZDLGDgsVRART4RYf1aFHUibNnnqo/A4RKVR0VfZthD3cUadbkiDP2b+g4Krq3vkMNm3+AStienXYw==";
        String tableName="demodatalake";


        
        AzureNamedKeyCredential credentials = new AzureNamedKeyCredential(accountName, accountKey);
        
        TableServiceClient tableServiceClient = new TableServiceClientBuilder()
        	    .endpoint("https://datalakesa99.table.core.windows.net/datalaketable99")
        		//.endpoint("https://datalakesa99.table.core.windows.net/")
        	    .credential(credentials)
        	    .buildClient(); 
        
        
        TableClient tableClient = tableServiceClient.getTableClient(tableName);
//        
//        TableEntity entity = new TableEntity("Mumbai", "3")
//        	    .addProperty("Name", "Marker Set")
//        	    .addProperty("Age", "20")
//        	    .addProperty("Qualification", "IT");
//        	tableClient.createEntity(entity);
        System.out.println("BEGIN2__________");
        
       // ListEntitiesOptions options = new ListEntitiesOptions().setFilter("Name" + " eq 'AAA'");
        System.out.println("BEGIN3__________");
        // Loop through the results, displaying information about the entities.
        tableClient.getEntity("Mumbai","3");
        TableEntity tableEntity =  tableClient.getEntity("Mumbai","3");

        System.out.printf("Retrieved entity with partition key '%s' and row key '%s'.", tableEntity.getPartitionKey(),
            tableEntity.getRowKey());
        
        	 System.out.println("END__________");
				
				 List<String> propertiesToSelect = new ArrayList<>();
				  propertiesToSelect.add("Name"); propertiesToSelect.add("Age");
				 
				  ListEntitiesOptions options = new ListEntitiesOptions()
				  .setFilter(String.format("PartitionKey eq '%s'", "Pune"))
				 .setSelect(propertiesToSelect);
				  
				  for (TableEntity entity : tableClient.listEntities(options, null, null)) {
				  Map<String, Object> properties = entity.getProperties();
				  System.out.printf("%s: %.2f%n", properties.get("Name"),
				  properties.get("Age")); }
				  
				 // "exception in thread "main" com.azure.data.tables.models.TableServiceException: Status code 501, "{"odata.error":{"code":"NotImplemented","message":{"lang":"en-US","value":"The requested operation is not implemented on the specified resource.\nRequestId:6dfd2db1-b002-0014-1778-4d0a62000000\nTime:2023-03-03T02:29:43.0536730Z"}}}"
				//	3at com.azure.data.tables.implementation.TableUtils.toTableServiceException(TableUtils.java:81)"
				  
        }
}


