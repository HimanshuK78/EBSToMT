package io.elastic.sagetomt;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.json.JsonObject;
import javax.json.JsonString;
public class Dbutils {
	public static PreparedStatement getPreparedStatement(String query,final JsonObject configuration)throws ClassNotFoundException
	,SQLException{
	    try {
	    	
	    	final JsonString usernamestring = configuration.getJsonString("username");
    	    final JsonString passwordstring = configuration.getJsonString("password");
    	    final JsonString databasednsstring = configuration.getJsonString("databasedns");
    	    final JsonString databasehoststring = configuration.getJsonString("databasehost");
    	    final JsonString databaseportstring = configuration.getJsonString("databaseport");
    	    String username=usernamestring.getString();
    	    String password=passwordstring.getString();
    	    String databasedns=databasednsstring.getString();
    	    String databasehost=databasehoststring.getString();
    	    String portno=databaseportstring.getString();
	    
	   	 Class.forName("com.pervasive.jdbc.v2.Driver");	   		
	   	 //Connection con =DriverManager.getConnection("jdbc:pervasive://66.199.227.124:1583/sageodbc","Peachtree","admin786");
	     Connection con =DriverManager.getConnection("jdbc:pervasive://"+databasehost+":"+portno+"/"+databasedns,username,password);
	   	 //System.out.println("DSN Connection ok."+databasehost+":"+portno+"/"+databasedns+username+password);
	   	PreparedStatement stmt=con.prepareStatement(query);
		return stmt;
	
} catch (Exception e) {
	      e.printStackTrace();
	    }
		return null; 
}

  public static Connection login(final JsonObject configuration )throws ClassNotFoundException
	,SQLException{
    	
    	 Connection con =null;
    	    try {
    	    final JsonString usernamestring = configuration.getJsonString("username");
    	    final JsonString passwordstring = configuration.getJsonString("password");
    	    final JsonString databasednsstring = configuration.getJsonString("databasedns");
    	    final JsonString databasehoststring = configuration.getJsonString("databasehost");
    	    final JsonString databaseportstring = configuration.getJsonString("databaseport");
    	    String username=usernamestring.getString();
    	    String password=passwordstring.getString();
    	    String databasedns=databasednsstring.getString();
    	    String databasehost=databasehoststring.getString();
    	    String portno=databaseportstring.getString();    	    
    	   	 Class.forName("com.pervasive.jdbc.v2.Driver");	   		
    	 	  con =DriverManager.getConnection("jdbc:pervasive://"+databasehost+":"+portno+"/"+databasedns,username,password);
    	   	//Connection con =DriverManager.getConnection("jdbc:pervasive://66.199.227.124:1583/sageodbc","Peachtree","admin786");
    	   	logger.info("DSN Connection {} ok."+con);
    	   	//System.out.println("DSN Connection ok."+con);    	   
    	    } catch (Exception e) {
    		      e.printStackTrace(); 		     
    		      
    		    }		
    	   return  con;
    }
    	
	
}
