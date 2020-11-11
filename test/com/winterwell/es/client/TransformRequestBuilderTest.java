package com.winterwell.es.client;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;
import com.winterwell.gson.FlexiGson;
import com.winterwell.utils.Dep;
import com.winterwell.utils.Printer;
import com.winterwell.utils.Utils;

public class TransformRequestBuilderTest {

	@Test
	public void testTransformPreview() {
		Dep.setIfAbsent(FlexiGson.class, new FlexiGson());
		Dep.setIfAbsent(ESConfig.class, new ESConfig());
		if ( ! Dep.has(ESHttpClient.class)) Dep.setSupplier(ESHttpClient.class, false, ESHttpClient::new);
		ESHttpClient esc = Dep.get(ESHttpClient.class);
		TransformRequestBuilder trb = esc.prepareTransformPreview();
		
		// specify some terms that we want to keep
		ArrayList<String> terms = new ArrayList<String>();
		terms.add("domain");
		terms.add("os");
		
		// specify source and destination
		trb.setBody("datalog.gl_sep20", "datalog.transformed", terms, "24h");
		trb.setDebug(true);
		IESResponse response = trb.get();
		Printer.out(response);
	}
	
	@Test
	public void testCreateTransform() {
		Dep.setIfAbsent(FlexiGson.class, new FlexiGson());
		Dep.setIfAbsent(ESConfig.class, new ESConfig());
		if ( ! Dep.has(ESHttpClient.class)) Dep.setSupplier(ESHttpClient.class, false, ESHttpClient::new);
		ESHttpClient esc = Dep.get(ESHttpClient.class);
		
		TransformRequestBuilder trb = esc.prepareTransform("transform_testjob"); //transform_testjob is the job ID
		
		// specify some terms that we want to keep
		ArrayList<String> terms = new ArrayList<String>();
		terms.add("domain");
		terms.add("os");
		
		// specify source and destination
		trb.setBody("datalog.gl_sep20", "datalog.test_transformed", terms, "24h");
		trb.setDebug(true);
		IESResponse response = trb.get();
		Printer.out(response);
	}
	
	@Test
	public void testStartTransform() {
		Dep.setIfAbsent(FlexiGson.class, new FlexiGson());
		Dep.setIfAbsent(ESConfig.class, new ESConfig());
		if ( ! Dep.has(ESHttpClient.class)) Dep.setSupplier(ESHttpClient.class, false, ESHttpClient::new);
		ESHttpClient esc = Dep.get(ESHttpClient.class);
		TransformRequestBuilder trb = esc.prepareTransformStart("transform_testjob"); 
		trb.setDebug(true);
		IESResponse response = trb.get();
		Printer.out(response);
	}

}
