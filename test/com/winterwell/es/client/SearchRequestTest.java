package com.winterwell.es.client;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.winterwell.es.ESTest;
import com.winterwell.es.client.query.ESQueryBuilders;
import com.winterwell.utils.Dep;

public class SearchRequestTest extends ESTest  {

	@BeforeClass
	public static void beforeClassInit() {
		BulkRequestBuilderTest brbt = new BulkRequestBuilderTest();
		List<String> ids = brbt.testBulkIndexMany2();
	}

//	@Test I think this is some BS Elastic custom extra
//	public void testQueryWithSettings() {
//		String index = BulkRequestBuilderTest.INDEX;
//		SearchRequest sr = getESJC().prepareSearch(index);
//		sr.setDebug(true);
//		sr.setSearchFields(Arrays.asList("name"));
//		sr.setQuery(ESQueryBuilders.simpleQueryStringQuery("name1"));
//		SearchResponse res = sr.get();
//		List<Map> hits = res.getHits();
//		System.out.println(hits);
//	}
	
	@Test
	public void testSetSize() {
		String index = BulkRequestBuilderTest.INDEX;
		// now search
		ESHttpClient esc = Dep.get(ESHttpClient.class);
		SearchRequest s = esc.prepareSearch(index);
		s.setSize(6);
		List<Map> hits = s.get().getHits();
		assert hits.size() == 6 : hits.size();
	}

	

}
