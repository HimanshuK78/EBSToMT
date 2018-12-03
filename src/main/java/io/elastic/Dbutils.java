package io.elastic;

import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.json.JsonObject;
import javax.json.JsonString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dbutils {

 private static final Logger logger = LoggerFactory.getLogger(Dbutils.class);

	public static PreparedStatement getPreparedStatement(String query,final JsonObject configuration)throws ClassNotFoundException
	,SQLException{
	    try {

	    	Connection con = Dbutils.getDatabaseConnection(configuration);
	    	
	      	PreparedStatement stmt=con.prepareStatement(query);
	      	
	      	logger.info("Database Connection {} ok." + con);
	      	
		    return stmt;
	
        } catch (Exception e) {
        	logger.info("Prepared Statement Error");
	      e.printStackTrace();
	    }
	    
		return null; 
    }

  public static Connection getDatabaseConnection(final JsonObject configuration )throws ClassNotFoundException
	,SQLException{
    	
		Connection con =null;

    	    try {
				
/*
    	     final JsonString databaseUrlString = configuration.getJsonString("databaseUrl");
        	 final JsonString usernameString = configuration.getJsonString("username");
     	     final JsonString passwordString = configuration.getJsonString("password");
     	    
		     String databaseUrl = databaseUrlString.getString();
			 String username = usernameString.getString();
			 String password = passwordString.getString();
     	    
			   */  
			  logger.info("now OracleDriver will be loaded");  
		      Class.forName("oracle.jdbc.driver.OracleDriver");	
			  logger.info("classes loaded successfully");   		
     	   	 con = DriverManager.getConnection("jdbc:oracle:thin:@apps.example.com:1521:EBSDB","apps","apps");
    	   	 logger.info("Database Connection {} ok." + con);
    	   	
    	    } catch (Exception e) {
    	    	logger.info("Database Connection Error");	    
    		      e.printStackTrace(); 		      
    		}	
    	    
    	   return  con;
    }
    		
}
