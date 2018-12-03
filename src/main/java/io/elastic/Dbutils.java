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
	    	
			
	    	 final JsonString databaseUrlString = configuration.getJsonString("databaseUrl");
        	 final JsonString usernameString = configuration.getJsonString("username");
     	     final JsonString passwordString = configuration.getJsonString("password");
     	    
		     String databaseUrl=databaseUrlString.getString();
			 String username=usernameString.getString();
			 String password=passwordString.getString();
     	    
     	        
     	   	 Class.forName("oracle.jdbc.driver.OracleDriver");	   		
     	   	 Connection con = (Connection)DriverManager.getConnection(databaseUrl,username,password);
	   	 //System.out.println("DSN Connection ok."+databasehost+":"+portno+"/"+databasedns+username+password);
	    
	      	PreparedStatement stmt=con.prepareStatement(query);
		    return stmt;
	
        } catch (Exception e) {
	      e.printStackTrace();
	    }
		return null; 
    }

  public static Connection getDatabaseConnection(final JsonObject configuration )throws ClassNotFoundException
	,SQLException{
    	
		Connection con =null;

    	    try {

    	     final JsonString databaseUrlString = configuration.getJsonString("databaseUrl");
        	 final JsonString usernameString = configuration.getJsonString("username");
     	     final JsonString passwordString = configuration.getJsonString("password");
     	    
		     String databaseUrl=databaseUrlString.getString();
			 String username=usernameString.getString();
			 String password=passwordString.getString();
     	    
     	        
     	   	 Class.forName("oracle.jdbc.driver.OracleDriver");	   		
     	   	 con = (Connection)DriverManager.getConnection(databaseUrl,username,password);    
            
    	   	//Connection con =DriverManager.getConnection("jdbc:pervasive://66.199.227.124:1583/sageodbc","Peachtree","admin786");
    	   	logger.info("Database Connection {} ok."+con);
    	   	//System.out.println("DSN Connection ok."+con);  

    	    } catch (Exception e) {
    		      e.printStackTrace(); 		     
    		      
    		    }		
    	   return  con;
    }
    		
}
