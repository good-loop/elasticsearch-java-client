package com.winterwell.es.client.admin;

import static org.junit.Assert.*;

import org.junit.Test;

import com.winterwell.es.ESTest;
import com.winterwell.es.ESType;
import com.winterwell.es.client.ESHttpClient;
import com.winterwell.es.client.IESResponse;
import com.winterwell.utils.Printer;
import com.winterwell.utils.Utils;

public class SearchSettingsRequestTest extends ESTest {
	
//	@Test TODO
	public void testSearchSettingsShow() {
		String idx = new PutMappingRequestBuilderTest().testPutMappingRequestBuilder();
		ESHttpClient esjc = getESJC();		
		SearchSettingsRequest ssr = new SearchSettingsRequest(esjc, idx);
		IESResponse r = ssr.get().check();
		System.out.println(r);
	}

}
