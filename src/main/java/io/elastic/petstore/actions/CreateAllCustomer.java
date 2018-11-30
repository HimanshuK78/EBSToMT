package io.elastic.sagetomt.actions;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.elastic.api.ExecutionParameters;
import io.elastic.api.InvalidCredentialsException;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.sagetomt.DatabaseAcess;

public class Getcompany implements Module {
    private static final Logger logger = LoggerFactory.getLogger(Getcompany.class);
   
    @Override
    public void execute(final ExecutionParameters parameters) {
        final JsonObject configuration = parameters.getConfiguration();          
             try {           
            DatabaseAcess dbobject = new DatabaseAcess();
            final JsonArray companylist=dbobject.getcompany(configuration);
            for(JsonValue companyobject : companylist)          
            {
                final JsonObject body = Json.createObjectBuilder()
                          .add("companylist",companyobject)
                          .build();
                  final Message data
                       = new Message.Builder().body(body).build();
                  logger.info("Emitting data"+data);        
        
            parameters.getEventEmitter().emitData(data);
            }
        
        }
            
        catch (Exception e) {
             logger.info("login Credentials are invalid");
            try {
                throw new InvalidCredentialsException("Failed to verify credentials", e);
            } catch (InvalidCredentialsException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
       

    }
    
    
}