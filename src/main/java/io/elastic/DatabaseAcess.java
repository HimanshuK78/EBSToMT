package io.elastic;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;


import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;

public class DatabaseAcess {

//	private static final Logger logger = LoggerFactory.getLogger(DatabaseAcess.class);

	public DatabaseAcess(){

	}

   	public JsonArray getCustomerList(final JsonObject configuration,String executiontime) {	
		   	
	ArrayList <String> keylist = new ArrayList<String>();
	JsonArrayBuilder customerList = Json.createArrayBuilder();
	JsonArray customerArray=null;
	String SelectCustomerQuery=null;

		try {	
			
		    SelectCustomerQuery = "select * from hz_parties";

		    PreparedStatement stmt = Dbutils.getPreparedStatement(SelectCustomerQuery,configuration);
		  	stmt.execute();
			ResultSet rs=stmt.getResultSet(); 

		  		for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
		   		{		  			
		  			keylist.add(rs.getMetaData().getColumnName(i));	  						   			
		   	     	   			
		   		}		  		
		  		
		  		while(rs.next()){	
		  			JsonObjectBuilder customerObject = Json.createObjectBuilder();	  			
		  			
		  			for(String keystring:keylist)
			   		{		  
		  			
		  				if(rs.getString(keystring)=="" || rs.getString(keystring)==null)
		  				{
		  					continue;
		  				}
		  			customerObject.add(keystring,rs.getString(keystring).toString());			  				
	  						   			
			   		}
		  			
		  			JsonObject tempvendorobject=customerObject.build();		  			
		  			
		  			customerList.add(tempvendorobject);			  			
		  		
		    		}//while
		  		
		  		 customerArray=customerList.build();

			} catch (Exception e) {
				e.printStackTrace();
			}
	
	 return customerArray;
		
	}

   	
   	public JsonArray createCustomer(final JsonObject configuration) {	
   		
   		return null;
   	}

  
}
