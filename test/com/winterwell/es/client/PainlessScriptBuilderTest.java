//package com.winterwell.es.client;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//
//import org.junit.Test;
//
//import com.winterwell.es.ESPath;
//import com.winterwell.es.ESTest;
//import com.winterwell.utils.Dep;
//import com.winterwell.utils.Printer;
//import com.winterwell.utils.containers.ArrayMap;
//import com.winterwell.utils.web.JsonPatchOp;
//import com.winterwell.utils.web.SimpleJson;
//
//public class PainlessScriptBuilderTest extends ESTest  {
//
//	@Test
//	public void testSimpleMap() {
//		PainlessScriptBuilder psb = new PainlessScriptBuilder();
//		Map<String, Object> jsonObject = new ArrayMap(
//				"a", "Apple", "n", 10, "x", 0.5);
//		psb.setJsonObject(jsonObject);
//		String script = psb.getScript();
//		Map params = psb.getParams();
//		System.out.println(script);
//		System.out.println(params);
//	}
//
//	@Test
//	public void testMapAndList() {
//		PainlessScriptBuilder psb = new PainlessScriptBuilder();
//		Map<String, Object> jsonObject = new ArrayMap(
//				"a", new String[] {"Apple"}, 
//				"n", Arrays.asList(10, 20));
//		psb.setJsonObject(jsonObject);
//		String script = psb.getScript();
//		Map params = psb.getParams();
//		System.out.println(script);
//		System.out.println(params);
//	}
//
//	@Test
//	public void testCallES() {
//		BulkRequestBuilderTest brbt = new BulkRequestBuilderTest();
//		brbt.testBulkIndex1();
//
//		PainlessScriptBuilder psb = new PainlessScriptBuilder();
//		Map<String, Object> jsonObject = new ArrayMap(
//				"a", new String[] {"Apple"}, 
//				"n", Arrays.asList(10, 20));
//		psb.setJsonObject(jsonObject);
//		String script = psb.getScript();
//		Map params = psb.getParams();
//		System.out.println(script);
//		System.out.println(params);
//		
//		ESHttpClient esjc = Dep.get(ESHttpClient.class);
//		
//		ESPath path = new ESPath("test", "thingy", "testCallES");
//		
//		esjc.prepareDelete(path).setRefresh(KRefresh.TRUE).get();
//		
//		esjc.prepareIndex(path).setBodyDoc(new ArrayMap("b", "Bee"))
//			.setRefresh("true")
//			.get();
//		
//		UpdateRequestBuilder up = esjc.prepareUpdate(path);
//		up.setRefresh(KRefresh.TRUE);
//		up.setScript(psb);
//		up.setDebug(true);
//		IESResponse resp = up.get();
//		resp.check();
//		System.out.println(resp.getJson());
//	}
//
//
//	@Test
//	public void testCallES_addToList() {
//		BulkRequestBuilderTest brbt = new BulkRequestBuilderTest();
//		brbt.testBulkIndex1();
//		
//		ESHttpClient esjc = Dep.get(ESHttpClient.class);
//		
//		ESPath path = new ESPath("test", "thingy", "testCallES_addToList");
//		
//		esjc.prepareDelete(path).setRefresh(KRefresh.TRUE).get();
//		
//		esjc.prepareIndex(path).setBodyDoc(
//				new ArrayMap("a", new String[] {"Avocado"}, "n", Arrays.asList(20)))
//			.setRefresh("true")
//			.get();
//
//		// save the object {a:[Apple], n:[10,20]} 
//		PainlessScriptBuilder psb = new PainlessScriptBuilder();
//		Map<String, Object> jsonObject = new ArrayMap(
//				"a", new String[] {"Apple"}, 
//				"n", Arrays.asList(10, 20));
//		psb.setJsonObject(jsonObject);
//		
//		UpdateRequestBuilder up = esjc.prepareUpdate(path);
//		up.setRefresh(KRefresh.TRUE);
//		up.setScript(psb);
//		up.setDebug(true);
//		IESResponse resp = up.get();
//		resp.check();
//		System.out.println(resp.getJson());
//		
//		// get
//		Map<String, Object> got = esjc.get(path);
//		Printer.out(got);
//		// ...merged?
//		List as = SimpleJson.getList(got, "a");
//		assert as.contains("Apple");
//		assert as.contains("Avocado");
//	}
//
//	
//	@Test
//	public void testCallES_setNull() {
//		BulkRequestBuilderTest brbt = new BulkRequestBuilderTest();
//		brbt.testBulkIndex1();
//		
//		ESHttpClient esjc = Dep.get(ESHttpClient.class);
//		
//		ESPath path = new ESPath(new String[]{"test"}, "testCallES_setNull");
//		
//		esjc.prepareDelete(path).setRefresh(KRefresh.TRUE).get();
//		
//		esjc.prepareIndex(path).setBodyDoc(
//				new ArrayMap("a", new String[] {"Avocado"}, "n", Arrays.asList(20)))
//			.setRefresh("true")
//			.get();
//
//		Map<String, Object> jsonObject = new ArrayMap("n", Arrays.asList(10, 20));
//		// diff? using json-patch https://tools.ietf.org/html/rfc6902
////		[
////	     { "op": "test", "path": "/a/b/c", "value": "foo" },
////	     { "op": "remove", "path": "/a/b/c" },
////	     { "op": "add", "path": "/a/b/c", "value": [ "foo", "bar" ] },
////	     { "op": "replace", "path": "/a/b/c", "value": 42 },
////	     { "op": "move", "from": "/a/b/c", "path": "/a/b/d" },
////	     { "op": "copy", "from": "/a/b/d", "path": "/a/b/e" }
////	   ]
//		List<JsonPatchOp> diffs = Arrays.asList(
//			JsonPatchOp.remove("/a")
//		);
//		PainlessScriptBuilder psb = PainlessScriptBuilder.fromJsonPatchOps(diffs);
//		Printer.out(psb.toString());
//		
//		UpdateRequestBuilder up = esjc.prepareUpdate(path);
//		up.setRefresh(KRefresh.TRUE);
//		up.setScript(psb);
//		up.setDebug(true);
//		IESResponse resp = up.get();
//		resp.check();
//		System.out.println(resp.getJson());
//		
//		// get
//		Map<String, Object> got = esjc.get(path);
//		Printer.out(got);
//		// ...removed?
//		List as = SimpleJson.getList(got, "a");
//		assert as==null || ! as.contains("Avocado");
//	}
//
//	
//}
