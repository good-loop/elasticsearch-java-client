package com.winterwell.es.client;

import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;

import com.winterwell.es.ESType;
import com.winterwell.es.client.admin.CreateIndexRequest;
import com.winterwell.es.client.admin.PutMappingRequest;
import com.winterwell.es.client.query.ESQueryBuilder;
import com.winterwell.es.client.query.ESQueryBuilders;
import com.winterwell.es.fail.ESIndexAlreadyExistsException;
import com.winterwell.gson.FlexiGson;
import com.winterwell.utils.Dep;
import com.winterwell.utils.Printer;
import com.winterwell.utils.Utils;
import com.winterwell.utils.containers.ArrayMap;

public class TransformRequestBuilderTest {
	
	public final static String INDEX = "testtransform";

	// create some test data to be transformed
	@BeforeClass
	public static void setup() {
		
		Dep.setIfAbsent(FlexiGson.class, new FlexiGson());
		Dep.setIfAbsent(ESConfig.class, new ESConfig());
		if ( ! Dep.has(ESHttpClient.class)) Dep.setSupplier(ESHttpClient.class, false, ESHttpClient::new);
		ESHttpClient esc = Dep.get(ESHttpClient.class);
		try {
			CreateIndexRequest cir = esc.admin().indices().prepareCreate(INDEX);
			cir.get().check();
			Utils.sleep(100);
			// set properties mapping
			PutMappingRequest pm = esc.admin().indices().preparePutMapping(INDEX);
			ESType mytype = new ESType()
					.property("domain", ESType.keyword)
					.property("os", ESType.keyword)
					.property("browser", ESType.keyword)
					.property("count", new ESType(double.class))
					.property("user", ESType.keyword)
					;
			pm.setMapping(mytype);
			pm.setDebug(true);
			IESResponse resp = pm.get().check();
			Printer.out(resp.getJson());
		} catch (ESIndexAlreadyExistsException ex) {
			Printer.out("Index already exists, proceeding...");
		}
		
		BulkRequest bulk = esc.prepareBulk();
		
		IndexRequest pi = esc.prepareIndex(INDEX, "s_0");			
		pi.setBodyMap(new ArrayMap("domain", "good-loop.com", "os", "linux", "browser", "firefox", 
				"user", "amfewmtapuoiofrlumoh@trk", "count", 1));
		bulk.add(pi);
		
		pi = esc.prepareIndex(INDEX, "s_1");			
		pi.setBodyMap(new ArrayMap("domain", "good-loop.com", "os", "linux", "browser", "chrome", "user", "dan@test.com@email", "count", 1));
		bulk.add(pi);
		
		pi = esc.prepareIndex(INDEX, "s_2");			
		pi.setBodyMap(new ArrayMap("domain", "good-loop.com", "os", "windows", "browser", "chrome", "user", "dan@test.com@email", "count", 1));
		bulk.add(pi);
		
		bulk.setRefresh(KRefresh.WAIT_FOR);
		bulk.setDebug(true);
		BulkResponse br = bulk.get();
	}

	@Test
	public void testTransformPreview() {
		Dep.setIfAbsent(FlexiGson.class, new FlexiGson());
		Dep.setIfAbsent(ESConfig.class, new ESConfig());
		if ( ! Dep.has(ESHttpClient.class)) Dep.setSupplier(ESHttpClient.class, false, ESHttpClient::new);
		ESHttpClient esc = Dep.get(ESHttpClient.class);
		TransformRequest trb = esc.prepareTransformPreview();
		
		// specify some terms that we want to keep
		ArrayList<String> terms = new ArrayList<String>();
		terms.add("domain");
		terms.add("os");
		
		// specify some terms that we want to sum
		ArrayList<String> aggs = new ArrayList<String>();
		
		// specify source and destination
		trb.setBody(INDEX, "datalog.transformed", aggs, terms, "");
		trb.setDebug(true);
		IESResponse response = trb.get();
		
		// sanity check on whether the transformed data is correct
		Printer.out(response.getParsedJson().get("preview"));
	}
	
	@Test
	public void testTransform() {
		Dep.setIfAbsent(FlexiGson.class, new FlexiGson());
		Dep.setIfAbsent(ESConfig.class, new ESConfig());
		if ( ! Dep.has(ESHttpClient.class)) Dep.setSupplier(ESHttpClient.class, false, ESHttpClient::new);
		ESHttpClient esc = Dep.get(ESHttpClient.class);
		
		TransformRequest trb = esc.prepareTransform("transform_testjob"); //transform_testjob is the job ID
		
		// specify some terms that we want to keep
		ArrayList<String> terms = new ArrayList<String>();
		terms.add("domain");
		terms.add("os");
		
		// specify some terms that we want to sum
		ArrayList<String> aggs = new ArrayList<String>();
		
		// specify source and destination
		trb.setBody(INDEX, "datalog.test_transformed", aggs, terms, "");
		trb.setDebug(true);
		IESResponse response = trb.get();
		assert response.isAcknowledged();
		
		// start transform job
		TransformRequest trb2 = esc.prepareTransformStart("transform_testjob"); 
		trb2.setDebug(true);
		IESResponse response2 = trb2.get();
		assert response2.isAcknowledged();
		
		// stop transform job
		Utils.sleep(2000);
		TransformRequest trb3 = esc.prepareTransformStop("transform_testjob"); 
		trb3.setDebug(true);
		IESResponse response3 = trb3.get();
		assert response3.isAcknowledged();
		
		// delete transform job
		TransformRequest trb4 = esc.prepareTransformDelete("transform_testjob"); 
		trb4.setDebug(true);
		IESResponse response4 = trb4.get();
		assert response4.isAcknowledged();
	}

	
	@Test
	public void testTransformWithNoTrkQuery() {
		Dep.setIfAbsent(FlexiGson.class, new FlexiGson());
		Dep.setIfAbsent(ESConfig.class, new ESConfig());
		if ( ! Dep.has(ESHttpClient.class)) Dep.setSupplier(ESHttpClient.class, false, ESHttpClient::new);
		ESHttpClient esc = Dep.get(ESHttpClient.class);
		
		String tid = "transform_query_testjob";
		TransformRequest trb = esc.prepareTransform(tid);
				
		// specify some terms that we want to keep
		ArrayList<String> terms = new ArrayList<String>();
		terms.add("domain");
		terms.add("os");
		terms.add("user");
		
		// specify some terms that we want to sum
		ArrayList<String> aggs = new ArrayList<String>();
		
		// specify source and destination
		trb.setBody(INDEX, "datalog.test_query_transformed", aggs, terms, "");
				
		// filter out amfewmtapuoiofrlumoh@trk
		ESQueryBuilder noTrk = ESQueryBuilders.boolQuery().mustNot(
				ESQueryBuilders.regexp("user", ".+@trk")
		);
		trb.setQuery(noTrk);
		
		trb.setDebug(true);
		IESResponse response = trb.get();
		assert response.isAcknowledged();
		
		// start transform job
		TransformRequest trb2 = esc.prepareTransformStart(tid); 
		trb2.setDebug(true);
		IESResponse response2 = trb2.get();
		assert response2.isAcknowledged();
		
		// stop transform job
		Utils.sleep(2000);
		TransformRequest trb3 = esc.prepareTransformStop(tid); 
		trb3.setDebug(true);
		IESResponse response3 = trb3.get();
		assert response3.isAcknowledged();
		
		// delete transform job
		TransformRequest trb4 = esc.prepareTransformDelete(tid); 
		trb4.setDebug(true);
		IESResponse response4 = trb4.get();
		assert response4.isAcknowledged();
	}

}
