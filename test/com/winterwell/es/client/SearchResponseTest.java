package com.winterwell.es.client;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.winterwell.utils.Dep;
import com.winterwell.utils.containers.ArrayMap;
import com.winterwell.web.ajax.JThing;

/**
 * @tested {@link SearchResponse}
 * @author daniel
 *
 */
public class SearchResponseTest {


	@Test
	public void testGetHits_map() {
		BulkRequestBuilderTest brbt = new BulkRequestBuilderTest();
		List<String> ids = brbt.testBulkIndexMany2();
		String index = BulkRequestBuilderTest.INDEX;
		// now search
		ESHttpClient esc = Dep.get(ESHttpClient.class);
		SearchRequestBuilder s = esc.prepareSearch(index);
		s.setSize(6);
		
		List<Map> hits = s.get().getHits();

		assert hits.size() == 6 : hits.size();
	}
	
	@Test
	public void testGetHits_ESHit() {
		BulkRequestBuilderTest brbt = new BulkRequestBuilderTest();
		List<String> ids = brbt.testBulkIndexMany2();
		String index = BulkRequestBuilderTest.INDEX;
		// now search
		ESHttpClient esc = Dep.get(ESHttpClient.class);
		SearchRequestBuilder s = esc.prepareSearch(index);
		s.setSize(6);
		
		List<ESHit<ArrayMap>> hits = s.get().getHits(ArrayMap.class);

		assert hits.size() == 6 : hits.size();
		ESHit<ArrayMap> h0 = hits.get(0);
		JThing<ArrayMap> jt = h0.getJThing();
		ArrayMap pojo = jt.java();
		assert pojo != null;
		assert h0.getIndex().equals(index);
	}
}
