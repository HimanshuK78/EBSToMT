package io.elastic;


import io.elastic.api.CredentialsVerifier;
import io.elastic.api.InvalidCredentialsException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.Connection;

import javax.json.JsonString;
import javax.json.JsonObject;

/**
 * Implementation of {@link CredentialsVerifier} used to verfy that credentials provide by user
 * are valid. This is accomplished by sending a simple request to the Petstore API.
 * In case of a successful response (HTTP 200) we assume credentials are valid. Otherweise invalid.
 */
public class ApiKeyVerifier implements CredentialsVerifier {

    private static final Logger logger = LoggerFactory.getLogger(ApiKeyVerifier.class);
    
     @Override
    public void verify(final JsonObject configuration) throws InvalidCredentialsException {
       // logger.info("About to verify the provided API key by retrieving the user");
    	
    	 try {
             final JsonString databaseUrlString = configuration.getJsonString("databaseUrl");
        	 final JsonString usernameString = configuration.getJsonString("username");
     	     final JsonString passwordString = configuration.getJsonString("password");
     	    
     	    String databaseUrl=databaseUrlString.getString();
     	    String username=usernameString.getString();
     	    String password=passwordString.getString();
     	    
     	        
     	   	Class.forName("oracle.jdbc.driver.OracleDriver");	   		
     	   	Connection con = (Connection)DriverManager.getConnection(databaseUrl,username,password);    
                 	   	
     	   	logger.info("DSN Connection status {}"+ con);                    
            
        } catch (Exception e) {
        	
            throw new InvalidCredentialsException("Failed to verify credentials", e);
            
        }
    }      

}
