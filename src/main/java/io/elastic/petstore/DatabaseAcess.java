package io.elastic.sagetomt;
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

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
public class DatabaseAcess {
	ResultSet pors;
	private static DecimalFormat df2 = new DecimalFormat(".##");

	private static boolean isNullOrEmpty(String str) {
        if(str != null && !str.isEmpty())
            return false;
        return true;
    }

	private static boolean isNull(String str) {
        if(str != null)
            return false;
        return true;
    }




	public JsonArray getVendorlist(final JsonObject configuration,String executiontime) {		
	ArrayList <String> keylist = new ArrayList<String>();
	JsonArrayBuilder vendorlist = Json.createArrayBuilder();
	JsonArray vendorarray=null;
	String vendorquery=null;
		try {	
			
			if(executiontime==null)
			 {
				vendorquery="select v1.VendorID,v1.Name,v1.DefCashAcct_RecNum,v1.Email,v1.VendorType,v1.PhoneNumber,v1.FAXNumber,v1.FederalEIN,v1.UseStdTerms,v1.VendorRecordNumber,v1.OurAccountWithThem,v1.UnusedSalesTaxID,v1.GLAcntNumber,v1.DueDays,v1.DiscountDays,cast(v1.DiscountPercent as numeric(10,2)) as DiscountPercent,cast(v1.CreditLimitNotUsed as numeric(10,2)) as CreditLimitNotUsed,V1.TermsCOD,v2.AddressLine1,v2.AddressLine2,v2.City,v2.State,v2.Zip,v2.Country,v2.TaxCode from Vendors as v1 left join Address as v2 ON v1.VendorRecordNumber=v2.VendorRecordNumber";  
			 }
			else if(executiontime!=null) {
			   vendorquery="select v1.VendorID,v1.Name,v1.DefCashAcct_RecNum,v1.Email,v1.VendorType,v1.PhoneNumber,v1.FAXNumber,v1.FederalEIN,v1.UseStdTerms,v1.VendorRecordNumber,v1.OurAccountWithThem,v1.UnusedSalesTaxID,v1.GLAcntNumber,v1.DueDays,v1.DiscountDays,cast(v1.DiscountPercent as numeric(10,2)) as DiscountPercent,cast(v1.CreditLimitNotUsed as numeric(10,2)) as CreditLimitNotUsed,V1.TermsCOD,v2.AddressLine1,v2.AddressLine2,v2.City,v2.State,v2.Zip,v2.Country,v2.TaxCode from Vendors as v1 left join Address as v2 ON v1.VendorRecordNumber=v2.VendorRecordNumber where v1.LastSavedAt >='"+executiontime+"'";
		     }	
			PreparedStatement stmt=Dbutils.getPreparedStatement(vendorquery,configuration);
		  	    stmt.execute();
		  		ResultSet rs=stmt.getResultSet(); 
		  		for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
		   		{		  			
		  			keylist.add(rs.getMetaData().getColumnName(i));	  						   			
		   	     	   			
		   		}		  		
		  		
		  		while(rs.next()){	
		  			JsonObjectBuilder vendorobject = Json.createObjectBuilder();	  			
		  			
		  			for(String keystring:keylist)
			   		{		  
		  			
		  				if(rs.getString(keystring)=="" || rs.getString(keystring)==null)
		  				{
		  					continue;
		  				}
		  			vendorobject.add(keystring,rs.getString(keystring).toString());			  				
	  						   			
			   		}
		  			
		  			JsonObject tempvendorobject=vendorobject.build();		  			
		  			
		  			vendorlist.add(tempvendorobject);			  			
		  		
		    		}
		  		
		  		 vendorarray=vendorlist.build();
		  		

			} catch (Exception e) {
				e.printStackTrace();
			}
		
		
	 return vendorarray;
		
	}

	public JsonArray getPurchaseOrder(final JsonObject configuration) {		
	ArrayList <String> keylist = new ArrayList<String>();
	JsonArrayBuilder purchaseorderlist = Json.createArrayBuilder();
	JsonArray purchaseorderarray=null;
		try {	
			String poquery="select v1.VendorID,v1.VendorRecordNumber,v2.INV_POSOOrderNumber,v2.IsDropShip,cast(v2.MainAmount as numeric(10,2)) as OrderTotal,v2.LastPostedAt,v3.GLAcntNumber,cast(v3.Quantity as numeric(10,2)) as Quantity,cast(v3.QtyReceived as numeric(10,2)) as QtyReceived,v3.RowDescription,cast(v3.Amount as numeric(10,2)) as Amount,v3.ItemRecordNumber,v4.ItemID from Vendors as v1 left join JrnlHdr as v2 on v1.VendorRecordNumber=v2.CustVendId left join JrnlRow as v3 on v2.CustVendId =v3.VendorRecordNumber left join LineItem as v4 on v3.VendorRecordNumber=v4.VendorRecordNumber where v4.ItemID IS NOT NULL order by v2.LastPostedAt desc ";
		  		PreparedStatement stmt=Dbutils.getPreparedStatement(poquery,configuration);
		  	    stmt.execute();
		  		ResultSet rs=stmt.getResultSet(); 
		  		for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
		   		{		  			
		  			keylist.add(rs.getMetaData().getColumnName(i));	  						   			
		   	     	   			
		   		}		  		
		  		
		  		while(rs.next()){	
		  			JsonObjectBuilder poobject = Json.createObjectBuilder();		  			
		  			for(String keystring:keylist)
			   		{		  
		  			
		  			poobject.add(keystring,rs.getString(keystring).toString());			  				
	  						   			
			   		}
		  			
		  			JsonObject tempvendorobject=poobject.build();		  			
		  			
		  			purchaseorderlist.add(tempvendorobject);			  			
		  		
		    		}
		  		
		  		purchaseorderarray=purchaseorderlist.build();
		  		

		  		    } catch (Exception e) {
		  		      e.printStackTrace();
		  		    }
		
		
	 return purchaseorderarray;
		
	}

	public JsonArray getGLaccount(final JsonObject configuration,final ExecutionParameters parameters) {		
	ArrayList <String> keylist = new ArrayList<String>();
	JsonArrayBuilder glaccountlist = Json.createArrayBuilder();
	JsonArray glaccountarray=null;
	String snapglaccount=null;
	String glquery="";
	JsonObject snapshot=parameters.getSnapshot();   
		try {	
			
			   if(snapshot.get("glaccountsnap")!=null)
			    {
			        snapglaccount=snapshot.getString("glaccountsnap");
			    	glquery="select AccountDescription,AccountID,Case AccountType when 24 then 'EXPENSE_ACCOUNT' when 10 then 'AP_ACCOUNT' Else 'ACCOUNT' END As AccountType ,GLAcntNumber from chart where GLAcntNumber > "+snapglaccount+";";
			    }
			    else if(snapshot.get("glaccountsnap")==null)
			    {
			    	
			    	glquery="select AccountDescription,AccountID,Case AccountType when 24 then 'EXPENSE_ACCOUNT' when 10 then 'AP_ACCOUNT' Else 'ACCOUNT' END As AccountType ,GLAcntNumber from chart;";
			    }
			
		  		PreparedStatement stmt=Dbutils.getPreparedStatement(glquery,configuration);
		  	    stmt.execute();
		  		ResultSet rs=stmt.getResultSet(); 
		  		for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
		   		{		  			
		  			keylist.add(rs.getMetaData().getColumnName(i));	  						   			
		   	     	   			
		   		}		  		
		  		
		  		while(rs.next()){	
		  			JsonObjectBuilder globject = Json.createObjectBuilder();		  			
		  			for(String keystring:keylist)
			   		{		  
		  			
		  			globject.add(keystring,rs.getString(keystring).toString());			  				
	  						   			
			   		}
		  			snapglaccount=rs.getString("GLAcntNumber");
		  			JsonObject tempglobject=globject.build();		  			
		  			
		  			glaccountlist.add(tempglobject);			  			
		  		
		    		}
		  		
		  		snapshot = Json.createObjectBuilder().add("glaccountsnap",snapglaccount).build();
	    	    parameters.getEventEmitter().emitSnapshot(snapshot);
		  		
		  		glaccountarray=glaccountlist.build();
		  		

		  		    } catch (Exception e) {
		  		      e.printStackTrace();
		  		    }
		
		
	 return glaccountarray;
		
	}

	public JsonArray getpoinvoice(final JsonObject configuration) {		
		ArrayList <String> keylist = new ArrayList<String>();
		JsonArrayBuilder poinvoicelist = Json.createArrayBuilder();
		JsonArray poinvoicearray=null;
			try {	
				String poinvoicequery="select v1.CustVendId,v1.TransactionDate,v1.Reference as Invoice_No,v1.DateDue,v1.TermsDescription,v1.LastPostedAt,v1.AccountDescription,v2.GLAcntNumber,v2.RowDescription,cast(v2.Quantity as numeric(10,2)) as Quantity,cast(v2.QtyReceived as numeric(10,2))as QtyReceived,cast(v2.UnitCost as numeric(10,2)) as UnitCost,cast((v2.UnitCost*v2.Quantity ) as numeric(10,2)) as Amount,v3.ItemID from  jrnlhdr as v1 left join Jrnlrow as v2 on v1.Reference=v2.InvNumForThisTrx left join Lineitem as v3 on v2.ItemRecordNumber=v3.ItemRecordNumber where v3.ItemID IS NOT NULL order by v1.LastPostedAt desc";
					PreparedStatement stmt=Dbutils.getPreparedStatement(poinvoicequery,configuration);
					stmt.execute();
					ResultSet rs=stmt.getResultSet(); 
					for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
					{		  			
						keylist.add(rs.getMetaData().getColumnName(i));	  						   			
									
					}		  		
					
					while(rs.next()){	
						JsonObjectBuilder poinvoiceobject = Json.createObjectBuilder();		  			
						for(String keystring:keylist)
						{		  
						
							poinvoiceobject.add(keystring,rs.getString(keystring).toString());			  				
											
						}
						
						JsonObject temppoinvoiceobject=poinvoiceobject.build();		  			
						
						poinvoicelist.add(temppoinvoiceobject);			  			
					
						}
					
					poinvoicearray=poinvoicelist.build();
					

						} catch (Exception e) {
						e.printStackTrace();
						}
			
			
		return poinvoicearray;
			
	}

	public void getitemlist(final JsonObject configuration,final ExecutionParameters parameters,String itemexecutiontime) {		
		JsonArrayBuilder itemlist = Json.createArrayBuilder();	
		JsonArray itemlistarray=null;
		
		String itemquery=null;
		try {	
			
			if(itemexecutiontime==null)
			{
				itemquery="select ItemID,Category,Location,cast(OrderQty as numeric(10,2)) as OrderQty,ItemDescription,ItemRecordNumber,UPC_SKU,LaborCost,MasterItemID,ItemClass from Lineitem order by LastSavedAt desc;";	
			}
			else if(itemexecutiontime!=null)
			{
				itemquery="select ItemID,Category,Location,cast(OrderQty as numeric(10,2)) as OrderQty,ItemDescription,ItemRecordNumber,UPC_SKU,LaborCost,MasterItemID,ItemClass from Lineitem where LastSavedAt >= '"+itemexecutiontime+"'";	
			}
				
			PreparedStatement stmt=Dbutils.getPreparedStatement(itemquery,configuration);
			stmt.execute();
			ResultSet rs=stmt.getResultSet();	
					
			while(rs.next())
			{	
				JsonObjectBuilder itemlistobject = Json.createObjectBuilder();	  
				if(rs.getString("ItemID")=="" || rs.getString("ItemID")==null)
				{
					continue;
				}	
				itemlistobject.add("ItemID",isNullOrEmpty(rs.getString("ItemID"))?"N/A":rs.getString("ItemID"));
				itemlistobject.add("ItemDescription",isNullOrEmpty(rs.getString("ItemDescription"))?"N/A":rs.getString("ItemDescription")); 
				itemlistobject.add("ItemRecordNumber",isNullOrEmpty(rs.getString("ItemRecordNumber"))?"N/A":rs.getString("ItemRecordNumber"));
				itemlistobject.add("UPC_SKU",isNullOrEmpty(rs.getString("UPC_SKU"))?"N/A":rs.getString("UPC_SKU"));
				itemlistobject.add("MasterItemID",  isNullOrEmpty(rs.getString("MasterItemID"))?"N/A":rs.getString("MasterItemID"));	 	
				itemlistobject.add("ItemClass",isNullOrEmpty(rs.getString("ItemClass"))?"N/A":rs.getString("ItemClass")); 
				itemlistobject.add("Category",isNullOrEmpty(rs.getString("Category"))?"N/A":rs.getString("Category")); 
				itemlistobject.add("Location",isNullOrEmpty(rs.getString("Location"))?"N/A":rs.getString("Location")); 
				itemlistobject.add("Cost",isNullOrEmpty(rs.getString("LaborCost"))?"0.0":rs.getString("LaborCost")); 
				itemlistobject.add("OrderQty",isNullOrEmpty(rs.getString("OrderQty"))?"N/A":rs.getString("OrderQty"));  

				JsonObject templistobject=itemlistobject.build();		
				final JsonObject body = Json.createObjectBuilder()
					.add("itemobject",templistobject)
					.build();
				final Message data	= new Message.Builder().body(body).build();        
				parameters.getEventEmitter().emitData(data);
			
			}
		} catch (Exception e) {
				e.printStackTrace();
			}
			
	}

	public void getpoinvoicelist(final JsonObject configuration,final ExecutionParameters parameters,String poinvoiceexecutiontime) {		
		JsonArrayBuilder poinvoicelist = Json.createArrayBuilder();
		JsonArray poinvoicearray=null; 
		String poinvoicequery=null;
		
		try {
			if(poinvoiceexecutiontime==null)
			{
			poinvoicequery="Select * from (select v1.VendorRecordNumber,v2.PostOrder,v2.TransactionDate,v2.Reference,v2.DateDue,v2.TermsDescription,v2.LastPostedAt,v2.GLAcntNumber as APaccount,(Select max(v3.LinkToAnotherTrx) from JrnlRow as v3 where v3.PostOrder = v2.Postorder and v3.LinkToAnotherTrx  > 0 ) as PurchaseOrderNumber from Vendors as v1 inner join jrnlhdr as v2 on v1.VendorRecordNumber=v2.CustVendId where v2.Reference IS NOT NULL and v2.JrnlKey_Journal='4' and v2.LastPostedAt is not null) as tt where Isnull(PurchaseOrderNumber,0) > 0";	
			}			
			else if(poinvoiceexecutiontime!=null)
			{
				poinvoicequery="Select * from (select v1.VendorRecordNumber,v2.PostOrder,v2.TransactionDate,v2.Reference,v2.DateDue,v2.TermsDescription,v2.LastPostedAt,v2.GLAcntNumber as APaccount,(Select max(v3.LinkToAnotherTrx) from JrnlRow as v3 where v3.PostOrder = v2.Postorder and v3.LinkToAnotherTrx  > 0 ) as PurchaseOrderNumber from Vendors as v1 inner join jrnlhdr as v2 on v1.VendorRecordNumber=v2.CustVendId where v2.Reference IS NOT NULL and v2.JrnlKey_Journal='4' and v2.LastPostedAt >='"+poinvoiceexecutiontime+"') as tt where Isnull(PurchaseOrderNumber,0) > 0";	
			}			
	  		PreparedStatement stmt=Dbutils.getPreparedStatement(poinvoicequery,configuration);
	  	    stmt.execute();
	  		ResultSet rs=stmt.getResultSet();		  		
			while(rs.next())
			{	
				JsonObjectBuilder poinvoiceobject = Json.createObjectBuilder();		  			
					
				if(rs.getString("Reference")=="" || rs.getString("Reference")==null || rs.getString("Reference").isEmpty())
				{
					continue;
				}
				String itemQuery="select v2.GLAcntNumber,v2.RowDescription,v2.ItemRecordNumber,cast(v2.Quantity as numeric(10,2)) as Quantity,cast(v2.QtyReceived as numeric(10,2))as QtyReceived,cast(v2.UnitCost as numeric(10,2)) as UnitCost,cast((v2.UnitCost*v2.Quantity ) as numeric(10,2)) as Amount,v3.ItemID from Jrnlrow as v2 left join Lineitem as v3 on v2.ItemRecordNumber=v3.ItemRecordNumber where v2.InvNumForThisTrx="+"'"+rs.getString("Reference")+"'"+"and v2.IncludeInInvLedger='1'";
				PreparedStatement stmtline=Dbutils.getPreparedStatement(itemQuery,configuration);
				stmtline.execute();
				ResultSet rsline=stmtline.getResultSet();
				JsonArrayBuilder itemarray = Json.createArrayBuilder(); 

     			while(rsline.next())
		  	 	{   
					JsonObjectBuilder Itemobject = Json.createObjectBuilder();	
					Itemobject.add("GLAcntNumber",isNullOrEmpty(rsline.getString("GLAcntNumber"))?"N/A":rsline.getString("GLAcntNumber"));
					Itemobject.add("RowDescription",isNullOrEmpty(rsline.getString("RowDescription"))?"N/A":rsline.getString("RowDescription"));
					Itemobject.add("Quantity",isNullOrEmpty(rsline.getString("Quantity"))?0:rsline.getInt("Quantity"));
					Itemobject.add("QtyReceived",isNullOrEmpty(rsline.getString("QtyReceived"))?0:rsline.getInt("QtyReceived"));
					Itemobject.add("UnitCost",isNullOrEmpty(rsline.getString("UnitCost"))?0.0:rsline.getDouble("UnitCost"));
					Itemobject.add("Amount",isNullOrEmpty(rsline.getString("Amount"))?0.0:rsline.getDouble("Amount"));
					Itemobject.add("ItemRecordNumber",isNullOrEmpty(rsline.getString("ItemRecordNumber"))?"N/A":rsline.getString("ItemRecordNumber"));
					Itemobject.add("ItemID",isNullOrEmpty(rsline.getString("ItemID"))?"N/A":rsline.getString("ItemID"));
					JsonObject Itembuild=Itemobject.build();		  		
					itemarray.add(Itembuild);	        
  		   		}
		       
				JsonArray ItemArraybuild=itemarray.build();
				if(ItemArraybuild.size() > 0)
				{  
					poinvoiceobject.add("VendorRecordNumber",isNullOrEmpty(rs.getString("VendorRecordNumber"))?"N/A":rs.getString("VendorRecordNumber"));
					poinvoiceobject.add("TransactionDate",isNullOrEmpty(rs.getString("LastPostedAt"))?"N/A":rs.getString("LastPostedAt"));
					poinvoiceobject.add("Invoice_No",isNullOrEmpty(rs.getString("Reference"))?"N/A":rs.getString("Reference"));
					poinvoiceobject.add("DateDue", isNullOrEmpty(rs.getString("DateDue"))?"N/A":rs.getString("DateDue"));	
					poinvoiceobject.add("TermsDescription",isNullOrEmpty(rs.getString("TermsDescription"))?"N/A":rs.getString("TermsDescription"));	
					poinvoiceobject.add("LastPostedAt",isNullOrEmpty(rs.getString("LastPostedAt"))?"N/A":rs.getString("LastPostedAt"));
					poinvoiceobject.add("APaccount",isNullOrEmpty(rs.getString("APaccount"))?"N/A":rs.getString("APaccount"));
					poinvoiceobject.add("PONumber", isNullOrEmpty(rs.getString("PostOrder"))?"N/A":rs.getString("PostOrder"));
					poinvoiceobject.add("item", ItemArraybuild); 

					JsonObject temppoinvoiceobject=poinvoiceobject.build();		  

					final JsonObject body = Json.createObjectBuilder()
					.add("poinvoice",temppoinvoiceobject)
					.build();
					final Message data
					= new Message.Builder().body(body).build(); 	      	
					parameters.getEventEmitter().emitData(data); 
		 		}
		
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	public JsonArray getpolist(final JsonObject configuration) {	

		JsonArrayBuilder polist = Json.createArrayBuilder();	
		JsonArray polistarray=null;	 
	
		try {	
				String poheaderquery="select v1.VendorID,v1.VendorRecordNumber,v2.PostOrder,v2.IsDropShip,v2.LastPostedAt from Vendors as v1 left join JrnlHdr as v2 on v1.VendorRecordNumber=v2.CustVendId where v2.PostOrder IS NOT NULL order by v2.LastPostedAt desc";
				PreparedStatement stmt=Dbutils.getPreparedStatement(poheaderquery,configuration);
				stmt.execute();
				ResultSet rs=stmt.getResultSet();	
		  		
				while(rs.next())
				{	
					double tempamount=0.00;
					JsonObjectBuilder polistobject = Json.createObjectBuilder();		  			
						
					if(rs.getString("PostOrder")=="" || rs.getString("PostOrder")==null)
					{
						continue;
					}
					String itemQuery="select v3.GLAcntNumber,cast(v3.Quantity as numeric(10,2)) as Quantity,cast(v3.QtyReceived as numeric(10,2)) as QtyReceived,v3.RowDescription,cast(v3.Amount as numeric(10,2)) as Amount,v3.ItemRecordNumber,v4.ItemID from JrnlRow as v3 left join LineItem as v4 on v3.ItemRecordNumber=v4.ItemRecordNumber where v3.PostOrder = '"+rs.getString("PostOrder")+"' and v4.ItemID IS NOT NULL and v3.Amount>0";
					PreparedStatement stmtline=Dbutils.getPreparedStatement(itemQuery,configuration);
					stmtline.execute();
					ResultSet rsline=stmtline.getResultSet();
					JsonArrayBuilder itemarray = Json.createArrayBuilder(); 
					
					while(rsline.next())
					{   		
										
						if(rsline.getString("ItemID")==null || rsline.getString("ItemID")=="" )
						{
							continue;
						}
						
						// if(rsline.getString("ItemID")!=null && rsline.getString("ItemID")!="" )
						// {
						// 	ItemID=rsline.getString("ItemID");
						// }
						
						// if(rsline.getString("Amount")!=null && rsline.getString("Amount")!="" )
						// {
						// 	Amount=rsline.getString("Amount");
						// }
						
						tempamount += Double.parseDouble(rsline.getString("Amount"));
						
						// if(rsline.getString("QtyReceived")!=null && rsline.getString("QtyReceived")!="" )
						// {
						// 	QtyReceived=rsline.getString("QtyReceived");
						// }
						
						// if(rsline.getString("Quantity")!=null && rsline.getString("Quantity")!="" )
						// {
						// 	Quantity=rsline.getString("Quantity");
						// }
						// if(rsline.getString("RowDescription")!=null && rsline.getString("RowDescription")!="" )
						// {
						// 	RowDescription=rsline.getString("RowDescription");
						// }
						// if(rsline.getString("GLAcntNumber")!=null && rsline.getString("GLAcntNumber")!="" )
						// {
						// 	GLAcntNumber=rsline.getString("GLAcntNumber");
						// }
						
						
						JsonObjectBuilder Itemobject = Json.createObjectBuilder();	
						Itemobject.add("GLAcntNumber",isNullOrEmpty(rsline.getString("GLAcntNumber"))?"N/A":rsline.getString("GLAcntNumber"));
						Itemobject.add("RowDescription",isNullOrEmpty(rsline.getString("RowDescription"))?"N/A":rsline.getString("RowDescription"));
						Itemobject.add("Quantity",isNullOrEmpty(rsline.getString("Quantity"))?"N/A":rsline.getString("Quantity"));
						Itemobject.add("QtyReceived",isNullOrEmpty(rsline.getString("QtyReceived"))?"N/A":rsline.getString("QtyReceived"));
						Itemobject.add("Amount",isNullOrEmpty(rsline.getString("Amount"))?"0":rsline.getString("Amount"));
						Itemobject.add("ItemID",isNullOrEmpty(rsline.getString("ItemID"))?"N/A":rsline.getString("ItemID"));
						JsonObject Itembuild=Itemobject.build();		  		
						itemarray.add(Itembuild);	  
					}		       
					JsonArray ItemArraybuild=itemarray.build();
					if(ItemArraybuild.size() > 0)
					{ 
						polistobject.add("VendorID",isNullOrEmpty(rs.getString("VendorID"))?"N/A":rs.getString("VendorID"));
						polistobject.add("VendorRecordNumber",isNullOrEmpty(rs.getString("VendorRecordNumber"))?"N/A":rs.getString("VendorRecordNumber")); 
						polistobject.add("PostOrder",isNullOrEmpty(rs.getString("PostOrder"))?"N/A":rs.getString("PostOrder"));
						polistobject.add("IsDropShip",isNullOrEmpty(rs.getString("IsDropShip"))?"N/A":rs.getString("IsDropShip"));
						polistobject.add("MainAmount",  df2.format(tempamount));	 	
						polistobject.add("LastPostedAt",isNullOrEmpty(rs.getString("LastPostedAt"))?"N/A":rs.getString("LastPostedAt")); 
						polistobject.add("item", ItemArraybuild);
			
						JsonObject temppoobject=polistobject.build(); 	
						polist.add(temppoobject);
					}
		
				}
		  		
		 		polistarray=polist.build();		  		

		  	} catch (Exception e) {
		  		      e.printStackTrace();
		  	}
		
	 return polistarray;
		
	}
 
 	public ResultSet getpoheader(final JsonObject configuration,String executiontime) {		
	String poheaderquery =null;
		try {	
			   if(executiontime==null)
			   {
				    poheaderquery="select v1.VendorID,v1.VendorRecordNumber,v2.PostOrder,v2.Reference,v2.DateDue,v2.IsDropShip,v2.LastPostedAt from Vendors as v1 left join JrnlHdr as v2 on v1.VendorRecordNumber=v2.CustVendId where v2.PostOrder IS NOT NULL and v2.JournalEx='18' order by v2.LastPostedAt desc";  
			   }
			   else if(executiontime!=null)
			   {
				    poheaderquery="select v1.VendorID,v1.VendorRecordNumber,v2.PostOrder,v2.Reference,v2.DateDue,v2.IsDropShip,v2.LastPostedAt from Vendors as v1 left join JrnlHdr as v2 on v1.VendorRecordNumber=v2.CustVendId where v2.PostOrder IS NOT NULL and v2.JournalEx='18' and v2.LastPostedAt >='"+executiontime+"'";  
			   }			    
		 		PreparedStatement stmt=Dbutils.getPreparedStatement(poheaderquery,configuration);
		  	    stmt.execute();
		  	  pors=stmt.getResultSet();	

		  } catch (Exception e) {
		  		      e.printStackTrace();
		  		    }
		
		
	 return pors;
		
	}

public JsonObject getpowithitem(final JsonObject configuration,final ResultSet rs) {		
	JsonObject temppoobject=null;
	 
	Double Amount= 0.0;  
    String potype="N/A"; 
		try {			
  		  double tempamount=0.00;
  		  JsonObjectBuilder polistobject = Json.createObjectBuilder();  			
		  String itemQuery="select v3.GLAcntNumber,cast(v3.Quantity as numeric(10,2)) as Quantity,cast(v3.QtyReceived as numeric(10,2)) as QtyReceived,cast(v3.UnitCost as numeric(10,2)) as UnitCost,v3.RowDescription,cast(v3.Amount as numeric(10,2)) as Amount,v3.ItemRecordNumber,v4.ItemID from JrnlRow as v3 left join LineItem as v4 on v3.ItemRecordNumber=v4.ItemRecordNumber where v3.PostOrder = '"+rs.getString("PostOrder")+"'";
		  PreparedStatement stmtline=Dbutils.getPreparedStatement(itemQuery,configuration);
	  	  stmtline.execute();
	  	  ResultSet rsline=stmtline.getResultSet();
	     JsonArrayBuilder itemarray = Json.createArrayBuilder();
	         	     	
     	
     		
     	while(rsline.next())
		  	 {   		
	        		        	
	        	if(rsline.getString("ItemID")==null || rsline.getString("ItemID")=="" )
				{
					continue;
				}
	     
	        	
	        	tempamount += Double.parseDouble(rsline.getString("Amount"));
	        	
	        	
		  		JsonObjectBuilder Itemobject = Json.createObjectBuilder(); 
				Itemobject.add("GLAcntNumber",isNullOrEmpty(rsline.getString("GLAcntNumber"))?"N/A":rsline.getString("GLAcntNumber"));
				Itemobject.add("RowDescription",isNullOrEmpty(rsline.getString("RowDescription"))?"N/A":rsline.getString("RowDescription"));
				Itemobject.add("Quantity",isNullOrEmpty(rsline.getString("Quantity"))?0:rsline.getInt("Quantity"));
				Itemobject.add("QtyReceived",isNullOrEmpty(rsline.getString("QtyReceived"))?0:rsline.getInt("QtyReceived"));
				Itemobject.add("UnitCost",isNullOrEmpty(rsline.getString("UnitCost"))?0.0:rsline.getDouble("UnitCost"));
				Itemobject.add("Amount",isNullOrEmpty(rsline.getString("Amount"))?0.0:rsline.getDouble("Amount"));
				Itemobject.add("ItemID",isNullOrEmpty(rsline.getString("ItemID"))?"N/A":rsline.getString("ItemID"));
				Itemobject.add("ItemRecordNumber",isNullOrEmpty(rsline.getString("ItemRecordNumber"))?"N/A":rsline.getString("ItemRecordNumber"));

		  		 

		  		JsonObject Itembuild=Itemobject.build();		  		
		  		itemarray.add(Itembuild);	        
  		   }
		       
		 JsonArray ItemArraybuild=itemarray.build();
		 if(ItemArraybuild.size() > 0)
		 {
			

		if(rs.getString("IsDropShip").equals("0"))
		{
     		potype="Standard";
		}
     	
     	if(rs.getString("IsDropShip").equals("1"))
		{
     		potype="DropShip";
		}

		polistobject.add("VendorID",isNullOrEmpty(rs.getString("VendorID"))?"N/A":rs.getString("VendorID"));
		polistobject.add("VendorRecordNumber",isNullOrEmpty(rs.getString("VendorRecordNumber"))?"N/A":rs.getString("VendorRecordNumber"));
		polistobject.add("PostOrder",isNullOrEmpty(rs.getString("PostOrder"))?"N/A":rs.getString("PostOrder"));
		polistobject.add("IsDropShip",isNullOrEmpty(rs.getString("IsDropShip"))?"N/A":rs.getString("IsDropShip"));
		polistobject.add("potype", potype);	
		polistobject.add("DateDue",isNullOrEmpty(rs.getString("DateDue"))?"N/A":rs.getString("DateDue"));	
		polistobject.add("ponumber",isNullOrEmpty(rs.getString("Reference"))?"N/A":rs.getString("Reference"));
		polistobject.add("MainAmount",df2.format(tempamount));
		polistobject.add("LastPostedAt", isNullOrEmpty(rs.getString("LastPostedAt"))?"N/A":rs.getString("LastPostedAt"));
		polistobject.add("item", ItemArraybuild);	
  

		    temppoobject=polistobject.build();	 
		  			
		 }	  
		  		

		  		    } catch (Exception e) {
		  		      e.printStackTrace();
		  		    }		
		
	 return temppoobject;
		
}

public JsonObject getpuchaseinvoice(final JsonObject configuration,ResultSet rs) {		
	 
	JsonObject temppoinvoiceobject=null; 
	try 
	{
			JsonObjectBuilder poinvoiceobject = Json.createObjectBuilder();		  			
	  		String itemQuery="select v2.GLAcntNumber,v2.RowDescription,v2.ItemRecordNumber,cast(v2.Quantity as numeric(10,2)) as Quantity,cast(v2.QtyReceived as numeric(10,2))as QtyReceived,cast(v2.UnitCost as numeric(10,2)) as UnitCost,cast((v2.UnitCost*v2.Quantity ) as numeric(10,2)) as Amount,v3.ItemID from Jrnlrow as v2 left join Lineitem as v3 on v2.ItemRecordNumber=v3.ItemRecordNumber where v2.InvNumForThisTrx="+"'"+rs.getString("Reference")+"'"+"and v2.IncludeInInvLedger='1'";
			PreparedStatement stmtline=Dbutils.getPreparedStatement(itemQuery,configuration);
		  	stmtline.execute();
		  	ResultSet rsline=stmtline.getResultSet();
		    JsonArrayBuilder itemarray = Json.createArrayBuilder();
	     		
		 	while(rsline.next())
		    	{   	
					JsonObjectBuilder Itemobject = Json.createObjectBuilder();	
						
					Itemobject.add("GLAcntNumber",isNullOrEmpty(rsline.getString("GLAcntNumber"))?"N/A":rsline.getString("GLAcntNumber"));
					Itemobject.add("RowDescription",isNullOrEmpty(rsline.getString("RowDescription"))?"N/A":rsline.getString("RowDescription"));
					Itemobject.add("Quantity",isNullOrEmpty(rsline.getString("Quantity"))?0:rsline.getInt("Quantity"));				
					Itemobject.add("UnitCost",isNullOrEmpty(rsline.getString("UnitCost"))?0.0:rsline.getDouble("UnitCost"));
					Itemobject.add("Amount",isNullOrEmpty(rsline.getString("Amount"))?0.0:rsline.getDouble("Amount"));

					Itemobject.add("ItemRecordNumber",isNullOrEmpty(rsline.getString("ItemRecordNumber"))?"N/A":rsline.getString("ItemRecordNumber"));
					Itemobject.add("ItemID",isNullOrEmpty(rsline.getString("ItemID"))?"N/A":rsline.getString("ItemID"));
					Itemobject.add("QtyReceived",isNullOrEmpty(rsline.getString("QtyReceived"))?0:rsline.getInt("QtyReceived"));

					JsonObject Itembuild=Itemobject.build();		  		
					itemarray.add(Itembuild);	        
			 }
			       
			 JsonArray ItemArraybuild=itemarray.build();
			 if(ItemArraybuild.size() > 0)
			 {
				poinvoiceobject.add("VendorRecordNumber",isNullOrEmpty(rs.getString("VendorRecordNumber"))?"N/A":rs.getString("VendorRecordNumber"));
				poinvoiceobject.add("TransactionDate",isNullOrEmpty(rs.getString("TransactionDate"))?"N/A":rs.getString("TransactionDate"));
				poinvoiceobject.add("Invoice_No",isNullOrEmpty(rs.getString("Reference"))?"N/A":rs.getString("Reference"));
				poinvoiceobject.add("DateDue",isNullOrEmpty(rs.getString("DateDue"))?"N/A":rs.getString("DateDue"));	
				poinvoiceobject.add("TermsDescription",isNullOrEmpty(rs.getString("TermsDescription"))?"N/A":rs.getString("TermsDescription"));
				poinvoiceobject.add("LastPostedAt",isNullOrEmpty(rs.getString("LastPostedAt"))?"N/A":rs.getString("LastPostedAt"));
				poinvoiceobject.add("APaccount", isNullOrEmpty(rs.getString("APaccount"))?"N/A":rs.getString("APaccount"));
				poinvoiceobject.add("PONumber", isNullOrEmpty(rs.getString("PostOrder"))?"N/A":rs.getString("PostOrder")); 
				poinvoiceobject.add("item", ItemArraybuild);
			
			 	temppoinvoiceobject=poinvoiceobject.build();	         
			 }	
	  		
   } catch (Exception e) {
		  	
		  	      e.printStackTrace();
		  		    }
		
	return temppoinvoiceobject;	
}

 public void getbillpayment(final JsonObject configuration,final ExecutionParameters parameters,String paymentexecutiontime) {		
	 
	String Reference="N/A";
	String TransactionDate="N/A";	
	String RowDescription="N/A";	
	double Amount=0.0;
	double MainAmount=0.0;
	String LinkToAnotherTrx="N/A";
	String GLAcntNumber="N/A";
	String PaymentMethod="N/A";
	String Billno="N/A";
	String paymentquery=null;
	String checkno="";
	try 
	{	
		if(paymentexecutiontime==null)
			 paymentquery="SELECT PostOrder,Reference,cast(TotalInvoicePaid as numeric(10,2)) as MainAmount,TransactionDate,GLAcntNumber,PayMethodRecNum as PaymentMethod from jrnlhdr where JrnlKey_Journal='2' and LastPostedAt is not null order by LastPostedAt desc;";			
		else if(paymentexecutiontime!=null)
			 paymentquery="SELECT PostOrder,Reference,cast(TotalInvoicePaid as numeric(10,2)) as MainAmount,TransactionDate,GLAcntNumber,PaymentMethod from jrnlhdr where PostOrder IS NOT NULL and JrnlKey_Journal='2' and LastPostedAt >= '"+paymentexecutiontime+"'";	
		
		
					PreparedStatement stmt=Dbutils.getPreparedStatement(paymentquery,configuration);
			  	    stmt.execute();
			  		ResultSet rs=stmt.getResultSet();	
			  	
	  		while(rs.next())
	  		{	
	  			JsonObjectBuilder paymentobject = Json.createObjectBuilder();		  			
	  			
	  			if(rs.getString("PostOrder")=="" || rs.getString("PostOrder")==null)
				{
					continue;
				}
	  			
	  			
					  String billQuery="SELECT PostOrder,RowDescription,cast(Amount as numeric(10,2)) as Amount,LinkToAnotherTrx from JrnlRow where Amount > 0 and LinkToAnotherTrx <> 0 and PostOrder='"+rs.getString("PostOrder")+"'";
					  PreparedStatement stmtline=Dbutils.getPreparedStatement(billQuery,configuration);
				  	  stmtline.execute();
				  	  ResultSet rsline=stmtline.getResultSet();
				     JsonArrayBuilder billarray = Json.createArrayBuilder();
					      
				     	
	     		
			     	while(rsline.next())
					{   		
				        
			     		
				        	if(rsline.getString("LinktoAnotherTrx").equals("0") || rsline.getString("LinktoAnotherTrx")=="" || rsline.getString("LinktoAnotherTrx").isEmpty() )
							{
								continue;
							}				        	
				        	
				        	Billno=rsline.getString("LinktoAnotherTrx");				        	
				        	Amount=rsline.getDouble("Amount");				        	
					     	RowDescription=rsline.getString("RowDescription");							
					  		JsonObjectBuilder billobject = Json.createObjectBuilder();					  		
					  		billobject.add("RowDescription",RowDescription);
					  		billobject.add("BillNumber",Billno);
					  		billobject.add("Amount",Amount);
					  		JsonObject billbuild=billobject.build();		  		
					  		billarray.add(billbuild);	        
			  		 }
			       
			 JsonArray billArraybuild=billarray.build();
			if(billArraybuild.size() > 0)
			{  
				paymentobject.add("PostOrder",isNullOrEmpty(rs.getString("PostOrder"))?"N/A":rs.getString("PostOrder"));
					if(rs.getString("Reference")!=null && rs.getString("Reference")!="" )
					{
						Reference=rs.getString("Reference");
					}				     	
					String pattern="^[0-9]+";				     	
					
					if(Reference.matches(pattern) && Reference.length()<7)
					{
						Reference=rs.getString("Reference");
					}				     	
					else {				     		
						Reference=null;
					}	

					if(rs.getString("PaymentMethod").equals("8") || rs.getString("PaymentMethod").equals("2"))
					{
						PaymentMethod="ACH";
					}	
					else if(rs.getString("PaymentMethod").equals("3") || rs.getString("PaymentMethod").equals("4") || rs.getString("PaymentMethod").equals("5")||rs.getString("PaymentMethod").equals("6")||rs.getString("PaymentMethod").equals("7"))
					{
						PaymentMethod="CREDITCARD";
					}
					else {
						PaymentMethod="UNKNOWN";
					}

				paymentobject.add("Reference",Reference);
				paymentobject.add("MainAmount",isNullOrEmpty(rs.getString("MainAmount"))?0.0:rs.getDouble("MainAmount"));
				paymentobject.add("TransactionDate",isNullOrEmpty(rs.getString("TransactionDate"))?"N/A":rs.getString("TransactionDate"));	
				paymentobject.add("RowDescription",isNullOrEmpty(rs.getString("RowDescription"))?"N/A":rs.getString("RowDescription"));
				paymentobject.add("PaymentMethod",PaymentMethod);
				paymentobject.add("GLAcntNumber",isNullOrEmpty(rs.getString("GLAcntNumber"))?"N/A":rs.getString("GLAcntNumber")); 


			 paymentobject.add("Bills",billArraybuild);			 			  			
			 JsonObject temppaymentobject=paymentobject.build();		  			
			 final JsonObject body = Json.createObjectBuilder()
	 	                  .add("Billpayment",temppaymentobject)
	 	                  .build();
	 	          final Message data
	 	               = new Message.Builder().body(body).build(); 	      	
	         parameters.getEventEmitter().emitData(data);
			
			 }
		}
	  		JsonObject snapshot = parameters.getSnapshot(); 
	  		Date date = new Date();
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
			String executiontime=dateFormat.format(date);
    		snapshot = Json.createObjectBuilder().add("paymentexecutiontime",executiontime).build();    	    
    	    parameters.getEventEmitter().emitSnapshot(snapshot); 		
	  		
   } catch (Exception e) {
		  	
		  	      e.printStackTrace();
		  		    }
		
		
 } 

public ResultSet getinvoiceheader(final JsonObject configuration,String executiontime) {		
	String poinvoicequery =null;
	ResultSet  invoicers=null;
	try {
			
	if(executiontime==null)
	 {
		 poinvoicequery="Select * from (select v1.VendorRecordNumber,v2.PostOrder,v2.TransactionDate,v2.Reference,v2.DateDue,v2.TermsDescription,v2.LastPostedAt,v2.GLAcntNumber as APaccount,(Select max(v3.LinkToAnotherTrx) from JrnlRow as v3 where v3.PostOrder = v2.Postorder and v3.LinkToAnotherTrx  > 0 ) as PurchaseOrderNumber from Vendors as v1 inner join jrnlhdr as v2 on v1.VendorRecordNumber=v2.CustVendId where v2.Reference IS NOT NULL and v2.JrnlKey_Journal='4' and v2.LastPostedAt is not null) as tt where Isnull(PurchaseOrderNumber,0) = 0";	 
	 }		 
	 else if(executiontime!=null)
	 {
		 poinvoicequery="Select * from (select v1.VendorRecordNumber,v2.PostOrder,v2.TransactionDate,v2.Reference,v2.DateDue,v2.TermsDescription,v2.LastPostedAt,v2.GLAcntNumber as APaccount,(Select max(v3.LinkToAnotherTrx) from JrnlRow as v3 where v3.PostOrder = v2.Postorder and v3.LinkToAnotherTrx  > 0 ) as PurchaseOrderNumber from Vendors as v1 inner join jrnlhdr as v2 on v1.VendorRecordNumber=v2.CustVendId where v2.Reference IS NOT NULL and v2.JrnlKey_Journal='4' and v2.LastPostedAt >='"+executiontime+"')as tt where Isnull(PurchaseOrderNumber,0) = 0";	 
	 }
					 
			PreparedStatement stmt=Dbutils.getPreparedStatement(poinvoicequery,configuration);
		    stmt.execute();
			invoicers=stmt.getResultSet();	

		  } catch (Exception e) {
		  		      e.printStackTrace();
		  		    }
		
		
	 return invoicers;
		
	}
public JsonArray getcompany(final JsonObject configuration) {		
	ArrayList <String> keylist = new ArrayList<String>();
	JsonArrayBuilder companylist = Json.createArrayBuilder();
	JsonArray companyarray=null;
	String companyquery=null;
		try {
			
			companyquery="select CompanyName,WebSite,CompanyType,DBN from Company;";  
			
			PreparedStatement stmt=Dbutils.getPreparedStatement(companyquery,configuration);
		  	    stmt.execute();
		  		ResultSet rs=stmt.getResultSet(); 		  			  		
		  		
		  		while(rs.next()){	
		  			JsonObjectBuilder companyobject = Json.createObjectBuilder();	  			
		  						
		  				companyobject.add("CompanyName",rs.getString("CompanyName"));	
		  				companyobject.add("WebSite",rs.getString("WebSite"));
	  				
		  			JsonObject tempcompanyobject=companyobject.build();		  			
		  			
		  			companylist.add(tempcompanyobject);			  			
		  		
		    		}
		  		
		  		 companyarray=companylist.build();
		  		

		  		    } catch (Exception e) {
		  		      e.printStackTrace();
		  		    }
		
		
	 return companyarray;
		
	}



  
}
