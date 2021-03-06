package com.winterwell.es.client;

import java.util.Map;

import org.junit.Test;

import com.winterwell.es.ESPath;
import com.winterwell.es.ESTest;
import com.winterwell.es.client.admin.CreateIndexRequest;
import com.winterwell.utils.Printer;
import com.winterwell.utils.Utils;
import com.winterwell.utils.containers.ArrayMap;

public class UpdateRequestBuilderTest extends ESTest {
	
	@Test
	public void testPutUpdate() {
		ESHttpClient esjc = getESJC();
		esjc.debug = true;
		// make an index
		String v = Utils.getRandomString(3);
		String idx = "test_put_"+v;
		CreateIndexRequest cir = esjc.admin().indices().prepareCreate(idx);
		cir.get().check();
		Utils.sleep(100);					
		
		// now index an item
		IndexRequest irb = esjc.prepareIndex(idx, "test_id_1");
		irb.setBodyDoc(new ArrayMap(
			"foo", "hello",
			"bar", "world"
		));
		irb.setDebug(true);
		irb.setRefresh(KRefresh.TRUE);
		IESResponse resp2 = irb.get().check();
		Utils.sleep(100);
		
		// and update it
		ESPath path = new ESPath(idx, "test_id_1");
		UpdateRequest up = esjc.prepareUpdate(path);
		up.setDoc(new ArrayMap("bar", "Mars"));
		up.setDebug(true);
		up.setRefresh(KRefresh.TRUE);
		IESResponse resp3 = up.get().check();
		Printer.out(resp3);
		
		Map<String, Object> got = esjc.get(path);
		System.out.println(got);
		assert got.get("bar").equals("Mars");
		assert got.get("foo").equals("hello");
	}
	
}
