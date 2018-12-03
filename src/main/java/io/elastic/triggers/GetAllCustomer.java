package io.elastic.triggers;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import javax.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.InvalidCredentialsException;
import io.elastic.api.Message;
import io.elastic.api.Module;

import io.elastic.DatabaseAcess;



public class GetAllCustomer implements Module {

	private static final Logger logger = LoggerFactory.getLogger(GetAllCustomer.class);

	String vendorexecutiontime=null;

    @Override
    public void execute(final ExecutionParameters parameters) {

        final JsonObject configuration = parameters.getConfiguration();          
        JsonObject snapshot=parameters.getSnapshot();

        if(snapshot.get("vendorexecutiontime")!=null)
        {
        	vendorexecutiontime=snapshot.getString("vendorexecutiontime");
        	logger.info("snapshot=="+vendorexecutiontime);
        }
        try {           
            DatabaseAcess dbobject = new DatabaseAcess();
        	final JsonArray vendorlist=dbobject.getCustomerList(configuration,vendorexecutiontime);
    		for(JsonValue vendorobject : vendorlist)			
    		{
    			final JsonObject body = Json.createObjectBuilder()
    	                  .add("Vendorlist",vendorobject)
    	                  .build();
    	          final Message data
    	               = new Message.Builder().body(body).build();
    	          logger.info("Emitting data"+data);		
    	
                parameters.getEventEmitter().emitData(data);
    		}
    		
    		Date date = new Date();
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
			String executiontime=dateFormat.format(date);
    		snapshot = Json.createObjectBuilder().add("vendorexecutiontime",executiontime).build();
    	    logger.info("Emitting new snapshot {}", snapshot.toString());
    	    parameters.getEventEmitter().emitSnapshot(snapshot);
        }
            
        catch (Exception e) {
        	 logger.info("GetAllCustomer are invalid");
            try {
				throw new InvalidCredentialsException("GetAllCustomer are Error", e);
			} catch (InvalidCredentialsException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        }
        
    }
    

}
