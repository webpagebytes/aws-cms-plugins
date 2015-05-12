package com.webpagebytes.plugins;

import java.beans.PropertyDescriptor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.logging.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchDeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeletableItem;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.webpagebytes.cms.WPBAdminDataStorage;
import com.webpagebytes.cms.cmsdata.WPBAdminFieldKey;
import com.webpagebytes.cms.cmsdata.WPBAdminFieldStore;
import com.webpagebytes.cms.cmsdata.WPBAdminFieldTextStore;
import com.webpagebytes.cms.exception.WPBIOException;
import com.webpagebytes.cms.exception.WPBSerializerException;

public class WPBAwsSdbAdminDataStorage implements WPBAdminDataStorage {
	
	private static final Logger log = Logger.getLogger(WPBAwsSdbAdminDataStorage.class.getName());
	public static final String CONFIG_ACCESS_KEY = "accessKey";
	public static final String CONFIG_SECRET_KEY = "secretKey";
	public static final String CONFIG_PROTOCOL = "protocol";
	public static final String CONFIG_ENDPOINT = "endpoint";
	public static final String CONFIG_DOMAIN = "domain";
	public static final String CLASS_ATRIBUTE = "wpbclass";
	public static final Integer STRING_CHUNK_SIZE = 249;
	
	protected AmazonSimpleDBClient sdbClient;
	protected String domainName;
	
	public void initialize(Map<String, String> params) throws WPBIOException {

		String accessKey = params.get(CONFIG_ACCESS_KEY);
		String secretkey = params.get(CONFIG_SECRET_KEY);
		BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretkey);
		
		ClientConfiguration clientConfig = new ClientConfiguration();
		
		String protocolStr = params.get(CONFIG_PROTOCOL);
		if (protocolStr != null && protocolStr.length()>0)
		{
			Protocol protocol = Protocol.valueOf(params.get(CONFIG_PROTOCOL));
			if (protocol != null)
			{
				clientConfig.setProtocol(protocol);
			}
		}
		sdbClient = new AmazonSimpleDBClient(awsCredentials, clientConfig);		
		String endpoint = params.get(CONFIG_ENDPOINT);
		if (endpoint != null)
		{
			sdbClient.setEndpoint(endpoint);
		}
		
		domainName = params.get(CONFIG_DOMAIN);
		// create the domain if not exists
		CreateDomainRequest createDomainRequest = new CreateDomainRequest();
		createDomainRequest.setDomainName(domainName);
		sdbClient.createDomain(createDomainRequest);	
	}

	private<T> String createInternalKey(String key, Class<T> dataType)
	{
		return key + dataType.getSimpleName();
	}
	
	public<T> PutAttributesRequest createAttributesRequests(T object) throws WPBSerializerException
	{
		PutAttributesRequest putAttributesRequest = new PutAttributesRequest();
		putAttributesRequest.setDomainName(domainName);
		List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
		
		attributes.add( (new ReplaceableAttribute()).withName(CLASS_ATRIBUTE).withValue(object.getClass().getSimpleName()).withReplace(true));		
		
		try
		{
			Class<? extends Object> kind = object.getClass();			
			Field[] fields = kind.getDeclaredFields();
			for(Field field: fields)
			{
				boolean storeField = (field.getAnnotation(WPBAdminFieldKey.class) != null) || 
									 (field.getAnnotation(WPBAdminFieldStore.class) != null) ||
									 (field.getAnnotation(WPBAdminFieldTextStore.class) != null);
				if (storeField)
				{
					String fieldName = field.getName();
					PropertyDescriptor pd = new PropertyDescriptor(fieldName, kind);
					Object value = null;
					try
					{
						value = pd.getReadMethod().invoke(object);
					} catch (Exception e)
					{
						throw new WPBSerializerException("Cannot get property value", e);
					}
					if (null == value) value="";
					
					ReplaceableAttribute attribute = new ReplaceableAttribute();
					attribute.setReplace(true);
					attribute.setName(fieldName);
					if (field.getType() == Long.class)
					{
						attribute.setValue(value.toString());
					}
					if (field.getType() == Integer.class)
					{
						attribute.setValue(value.toString());
					}
					if (field.getType() == String.class && field.getAnnotation(WPBAdminFieldTextStore.class) != null)
					{
						String str = value.toString();
						int len = str.length();
						int index = 0;
						do
						{
							int chunk = len;
							if (len > STRING_CHUNK_SIZE)
							{
								chunk = STRING_CHUNK_SIZE;
							}
							String tempStr = str.substring(0, chunk);
							str = str.substring(chunk);
							len = str.length();

							ReplaceableAttribute chunkAttribute = new ReplaceableAttribute();
							chunkAttribute.setReplace(true);
							chunkAttribute.setName(fieldName + Integer.valueOf(index));
							
							chunkAttribute.setValue(tempStr);
							attributes.add(chunkAttribute);
							index = index + 1;

						} while (len != 0);
						attribute.setValue(String.valueOf(index));
					}
					if (field.getType() == String.class && field.getAnnotation(WPBAdminFieldTextStore.class) == null)
					{
						attribute.setValue(value.toString());
					}
					if (field.getType() == Date.class)
					{
						Long valueLong = (Long) ((Date)value).getTime();
						attribute.setValue(valueLong.toString());
					}
					attributes.add(attribute);
					
					if (field.getAnnotation(WPBAdminFieldKey.class) != null)
					{
						putAttributesRequest.setItemName(createInternalKey(value.toString(), kind));
					}
				}
			} 
		} catch (Exception e)
		{
			throw new WPBSerializerException("cannot de-serialize record", e);
		}
		putAttributesRequest.setAttributes(attributes);
		return putAttributesRequest;		
	}
	
	private String escapeDoubleQuote(String str)
	{
		return str.replaceAll("\"", "\"\"");
	}
	private String escapeBacktick(String str)
	{
		return "`" + str.replaceAll("`", "``") + "`";
	}
	private<T> String build_queryAll_statement(String selector, Class<T> dataClass)
	{
		return "select " + selector + " from " + escapeBacktick(this.domainName) + " where " + CLASS_ATRIBUTE + "=\"" + escapeDoubleQuote(dataClass.getSimpleName()) +"\" ";
	}
	private String build_sort_statement(String property, AdminSortOperator operator)
	{
		if (operator == AdminSortOperator.NO_SORT) return " ";
		// sort needs to include the sort property in the where clause with a non null predicate
		// to workaround this we will add a != with a random guid, not ideal but it's a solution
		String random = UUID.randomUUID().toString();
		if (operator == AdminSortOperator.ASCENDING) return " and " + property + "!=\"" + random + "\" " + " order by " + escapeBacktick(property) + " asc ";
		return " and " + property + "!=\"" + random + "\" " + " order by " + escapeBacktick(property) + " desc ";		
	}
	
	private<T> String build_queryAll_statement(Class<T> dataClass, String property, AdminSortOperator operator)
	{
		return "select * from " + escapeBacktick(this.domainName) + " where " + CLASS_ATRIBUTE + "=\"" + escapeDoubleQuote(dataClass.getSimpleName()) +"\" "
	           + build_sort_statement(property, operator);
	}
	private String queryOperatorToString(AdminQueryOperator operator)
	{
		if (operator == AdminQueryOperator.EQUAL)
		{
			return "=";
		} else if (operator == AdminQueryOperator.GREATER_THAN)
		{
			return ">";
		} else if (operator == AdminQueryOperator.LESS_THAN)
		{
			return "<";
		} else if (operator == AdminQueryOperator.GREATER_THAN_OR_EQUAL)
		{
			return ">=";
		} else if (operator == AdminQueryOperator.LESS_THAN_OR_EQUAL)
		{
			return "<=";
		} else if (operator == AdminQueryOperator.NOT_EQUAL)
		{
			return "!=";
		}
		return " invalid operator ";
	}
	

	private<T> String build_query_with_condition(String selector, Class<T> dataClass, Set<String> properties,
			Map<String, AdminQueryOperator> operators, Map<String, Object> parameters)
	{
		String query = "select " + selector + " from " + escapeBacktick(this.domainName) + " where " + CLASS_ATRIBUTE + "=\"" + escapeDoubleQuote(dataClass.getSimpleName()) +"\" " ;
		for(String property: properties)
		{
			query += (" and " +  escapeBacktick(property) + queryOperatorToString(operators.get(property)) +"\"" + escapeDoubleQuote(parameters.get(property).toString()) + "\"");
		}
		query += " ";
		return query;
	}

	private<T> String build_query_with_condition_and_sort(String selector,Class<T> dataClass, Set<String> properties,
			Map<String, AdminQueryOperator> operators, Map<String, Object> parameters, String sortProperty,
			AdminSortOperator sortOperator)
	{
		String query = build_query_with_condition(selector, dataClass, properties, operators, parameters);
		query += build_sort_statement(sortProperty, sortOperator);
		return query;
	}

	
	public<T> T copyAttributesToInstance(Class<T> kind, List<Attribute> attributes) throws WPBSerializerException
	{
		if (attributes.size() == 0) return null;
		
		//keep a map of attributes for WPB strings fields  
		Map<String, String> attributesMap = new HashMap<String,String>();
		for (int i = 0; i< attributes.size(); i++)
		{
			String attributeName = attributes.get(i).getName();
			String attributeValue = attributes.get(i).getValue();
			attributesMap.put(attributeName, attributeValue);
		}
		
		try
		{
			T result = kind.newInstance();
			for (int i = 0; i< attributes.size(); i++)
			{
				String attributeName = attributes.get(i).getName();
				String attributeValue = attributes.get(i).getValue();
				
				Field[] fields = kind.getDeclaredFields();
				Field field = null;
				for(Field afield: fields)
				{
					afield.setAccessible(true);
					if (afield.getName().equals(attributeName))
					{
						field = afield;
						break;
					}
				}
				if (field == null) continue;
				
				boolean storeField = (field.getAnnotation(WPBAdminFieldKey.class) != null) || 
						 (field.getAnnotation(WPBAdminFieldStore.class) != null) ||
						 (field.getAnnotation(WPBAdminFieldTextStore.class) != null);
				if (storeField)
				{
					String fieldName = field.getName();
					PropertyDescriptor pd = new PropertyDescriptor(fieldName, kind);
			
					if (field.getType() == Long.class)
					{
						Long value = null;
						if (attributeValue != null && attributeValue.length()>0)
						{
							value = Long.valueOf(attributeValue);
						}
						pd.getWriteMethod().invoke(result, value);
					} else
					if (field.getType() == String.class && field.getAnnotation(WPBAdminFieldTextStore.class) != null)
					{
						String finalValue = "";
						int chunks = Integer.valueOf(attributeValue);
						for(int x = 0; x < chunks; x++)
						{
							finalValue = finalValue + attributesMap.get(fieldName + String.valueOf(x));
						}
						pd.getWriteMethod().invoke(result, finalValue);
					} else
					if (field.getType() == String.class && field.getAnnotation(WPBAdminFieldTextStore.class) == null)
					{
						pd.getWriteMethod().invoke(result, attributeValue);
					} else
					if (field.getType() == Integer.class)
					{
						Integer value = null;
						if (attributeValue != null && attributeValue.length()>0)
						{
							value = Integer.valueOf(attributeValue);
						}
						pd.getWriteMethod().invoke(result, value);
					} else
					if (field.getType() == Date.class)
					{
						Long value = null;
						if (attributeValue != null && attributeValue.length()>0)
						{
							value = Long.valueOf(attributeValue);
						}
						Date date = new Date(value);
						pd.getWriteMethod().invoke(result, date);
					}						
					
				}
			}
			return result;
		} catch (Exception e)
		{
			throw new WPBSerializerException("cannot serialize record " + kind.getSimpleName(), e);
		}
	}
	
	public <T> void delete(String recordid, Class<T> dataClass)
			throws WPBIOException {
		try
		{
			DeleteAttributesRequest deleteAttributesRequest = new DeleteAttributesRequest(domainName, createInternalKey(recordid, dataClass));
			sdbClient.deleteAttributes(deleteAttributesRequest);
		} catch (Exception e)
		{
			throw new WPBIOException("Cannot delete record " + recordid, e);
		}		
	}


	public <T> void delete(Class<T> dataClass, String property,
			AdminQueryOperator operator, Object parameter)
			throws WPBIOException {

		SelectRequest selectRequest = new SelectRequest();		
		Set<String> properties = new HashSet<String>();
		properties.add(property);
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put(property, parameter);
		
		Map<String, AdminQueryOperator> operators = new HashMap<String, AdminQueryOperator>();
		operators.put(property, operator);
		selectRequest.withConsistentRead(true).withSelectExpression(build_query_with_condition("itemName()", dataClass, properties, operators, parameters));
		internalDelete(selectRequest);
	}

	private void internalDelete(SelectRequest selectRequest) throws WPBIOException
	{
		List<DeletableItem> records = new ArrayList<DeletableItem>();		
		int batchSize = 25;
		int count = 0;
		try
		{
			SelectResult selectResult = null;
			do 
			{
				if (selectResult != null)
				{
					selectRequest.setNextToken(selectResult.getNextToken());
				}
				selectResult = sdbClient.select(selectRequest);
				List<Item> items = selectResult.getItems();
				for(Item item: items)
				{
					if (count == batchSize)
					{
						BatchDeleteAttributesRequest batchDeleteAttributeRequest = new BatchDeleteAttributesRequest(domainName, records);
						sdbClient.batchDeleteAttributes(batchDeleteAttributeRequest);
						records.clear();
						count = 0;
					}
					records.add((new DeletableItem()).withName(item.getName()));
					count += 1;
				}				
			} while (selectResult.getNextToken() != null);
			if (count>0)
			{
				BatchDeleteAttributesRequest batchDeleteAttributeRequest = new BatchDeleteAttributesRequest(domainName, records);
				sdbClient.batchDeleteAttributes(batchDeleteAttributeRequest);
				records.clear();				
			}			
		} catch (Exception e)
		{
			throw new WPBIOException("cannot delete records :" + selectRequest.getSelectExpression(), e);
		}					
	}
	
	private<T> List<T> internalQuery(Class<T> dataClass, SelectRequest selectRequest) throws WPBIOException
	{
		List<T> result = new ArrayList<T>();
		try
		{
			SelectResult selectResult = null;
			do 
			{
				if (selectResult != null)
				{
					selectRequest.setNextToken(selectResult.getNextToken());
				}
				selectResult = sdbClient.select(selectRequest);
				List<Item> items = selectResult.getItems();
				for(Item item: items)
				{
					T t = copyAttributesToInstance(dataClass, item.getAttributes());
					result.add(t);
				}
				
			} while (selectResult.getNextToken() != null);
		} catch (Exception e)
		{
			throw new WPBIOException("cannot get all records " + dataClass.getSimpleName(), e);
		}
		
		return result;		
	}
	
	public <T> List<T> getAllRecords(Class<T> dataClass) throws WPBIOException {
		SelectRequest selectRequest = new SelectRequest();
		selectRequest.withConsistentRead(true).withSelectExpression(build_queryAll_statement("*", dataClass));
		return internalQuery(dataClass, selectRequest);
	}

	public <T> List<T> getAllRecords(Class<T> dataClass, String property,
			AdminSortOperator operator) throws WPBIOException {
		SelectRequest selectRequest = new SelectRequest();
		selectRequest.withConsistentRead(true).withSelectExpression(build_queryAll_statement(dataClass, property, operator));
		return internalQuery(dataClass, selectRequest);
	}

	public <T> T add(T record) throws WPBIOException {
		try
		{
			PutAttributesRequest putAttributesRequest = createAttributesRequests(record);
			
			sdbClient.putAttributes(putAttributesRequest);
						
		} catch (Exception e)
		{
			throw new WPBIOException("cannot create record", e);
		}
		return record;
	}

	public <T> T addWithKey(T record) throws WPBIOException {
		try
		{
			PutAttributesRequest putAttributesRequest = createAttributesRequests(record);
			
			sdbClient.putAttributes(putAttributesRequest);
						
		} catch (Exception e)
		{
			throw new WPBIOException("cannot create record with key ", e);
		}
		return record;
	}

	public <T> T get(String recordid, Class<T> dataClass) throws WPBIOException {
		try
		{
			GetAttributesRequest getAttributesRequest = new GetAttributesRequest(domainName, createInternalKey(recordid, dataClass));
			GetAttributesResult attributesResult = sdbClient.getAttributes(getAttributesRequest);
			return copyAttributesToInstance(dataClass, attributesResult.getAttributes());
		} catch (Exception e)
		{
			throw new WPBIOException("Cannot get record " + recordid, e);
		}	
	}

	public <T> T update(T record) throws WPBIOException {
		return addWithKey(record);
	}

	public <T> List<T> query(Class<T> dataClass, String property,
			AdminQueryOperator operator, Object parameter)
			throws WPBIOException {		
		SelectRequest selectRequest = new SelectRequest();
		Set<String> properties = new HashSet<String>();
		properties.add(property);
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put(property, parameter);
		
		Map<String, AdminQueryOperator> operators = new HashMap<String, AdminQueryOperator>();
		operators.put(property, operator);
		
		selectRequest.withConsistentRead(true).withSelectExpression(build_query_with_condition("*", dataClass, properties, operators, parameters));
		return internalQuery(dataClass, selectRequest);
	}

	public <T> List<T> queryEx(Class<T> dataClass, Set<String> propertyNames,
			Map<String, AdminQueryOperator> operators,
			Map<String, Object> values) throws WPBIOException {
		SelectRequest selectRequest = new SelectRequest();		
		selectRequest.withConsistentRead(true).withSelectExpression(build_query_with_condition("*", dataClass, propertyNames, operators, values));
		return internalQuery(dataClass, selectRequest);
	}

	public <T> List<T> queryWithSort(Class<T> dataClass, String property,
			AdminQueryOperator operator, Object parameter, String sortProperty,
			AdminSortOperator sortOperator) throws WPBIOException {
		
		SelectRequest selectRequest = new SelectRequest();
		Set<String> properties = new HashSet<String>();
		properties.add(property);
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put(property, parameter);
		
		Map<String, AdminQueryOperator> operators = new HashMap<String, AdminQueryOperator>();
		operators.put(property, operator);
		
		selectRequest.withConsistentRead(true).withSelectExpression(build_query_with_condition_and_sort("*", dataClass, properties, operators, parameters, sortProperty, sortOperator));
		return internalQuery(dataClass, selectRequest);		
	}

	public <T> void deleteAllRecords(Class<T> dataClass) throws WPBIOException {
		SelectRequest selectRequest = new SelectRequest();		
		selectRequest.withConsistentRead(true).withSelectExpression(build_queryAll_statement("itemName()", dataClass));
		internalDelete(selectRequest);
	}

}
