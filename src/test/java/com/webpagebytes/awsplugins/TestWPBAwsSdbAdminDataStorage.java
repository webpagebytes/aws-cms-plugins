package com.webpagebytes.awsplugins;

import static org.junit.Assert.*;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.webpagebytes.awsplugins.WPBAwsSdbAdminDataStorage;
import com.webpagebytes.cms.cmsdata.WPBUri;


@RunWith(PowerMockRunner.class)
@PrepareForTest({WPBAwsSdbAdminDataStorage.class})
public class TestWPBAwsSdbAdminDataStorage {

public WPBAwsSdbAdminDataStorage sdbDataStorage = new WPBAwsSdbAdminDataStorage();
private AmazonSimpleDBClient sdbClientMock = EasyMock.createMock(AmazonSimpleDBClient.class);

@Before
public void before()
{
	String domain = "test";
	Whitebox.setInternalState(sdbDataStorage, "sdbClient", sdbClientMock);
	Whitebox.setInternalState(sdbDataStorage, "domainName", domain);
}



@Test
public void test_escapeDoubleQuote()
{
	try
	{
		String result = Whitebox.invokeMethod(sdbDataStorage, "escapeDoubleQuote", "Te\"st");
		assertTrue (result.equals("Te\"\"st"));

		result = Whitebox.invokeMethod(sdbDataStorage, "escapeDoubleQuote", "\"\"");
		assertTrue (result.equals("\"\"\"\""));

	} catch (Exception e)
	{
		assertTrue (false);
	}
}

@Test
public void test_escapeBacktick()
{
	try
	{
		String result = Whitebox.invokeMethod(sdbDataStorage, "escapeBacktick", "Te`st");
		assertTrue (result.equals("`Te``st`"));

		result = Whitebox.invokeMethod(sdbDataStorage, "escapeBacktick", "``");
		assertTrue (result.equals("``````"));

	} catch (Exception e)
	{
		assertTrue (false);
	}
}

@Test
public void test_build_queryAll_statement()
{
	try
	{
		String result = Whitebox.invokeMethod(sdbDataStorage, "build_queryAll_statement", "*", WPBUri.class);
		assertTrue (result.equals("select * from `test` where wpbclass=\"WPBUri\" "));
	} catch (Exception e)
	{
		assertTrue (false);
	}
}



}

