package com.winterwell.es.client;

import java.util.ArrayList;
import com.winterwell.utils.containers.ArrayMap;

/**
 * Transform indices to merge fields.
 * See https://www.elastic.co/guide/en/elasticsearch/reference/7.9/transforms.html
 */

public class TransformRequestBuilder extends ESHttpRequest<TransformRequestBuilder, IESResponse> {
	
	// called when a transform preview is requested
	public TransformRequestBuilder(ESHttpClient esHttpClient) {
		super(esHttpClient, "_transform/_preview");
		setIndex(null); 
		method = "POST";
	}
	
	// called when creating and starting a transform job request
	public TransformRequestBuilder(ESHttpClient esHttpClient, String transform_job, String request) {
		super(esHttpClient, "_transform/"+transform_job);
		setIndex(null); 
		method = request; 
	}
	
	/**
	 * Set the body of a transform request
	 * @param src index source
	 * @param dest index destination
	 * @param terms a list of terms which you want to group by
	 * @param interval if want to group by date histogram, specify interval, else pass empty string
	 * @return this
	*/
	public TransformRequestBuilder setBody(String src, String dest, ArrayList<String> terms, String interval) {
		ArrayMap group_by = new ArrayMap();
		ArrayMap aggregations = new ArrayMap("count", new ArrayMap("value_count", new ArrayMap("field", "count")));
		for (String term : terms) {
			group_by.put(term, new ArrayMap("terms", new ArrayMap("field", term)));
		}
		if (interval != "") {
			group_by.put("time", new ArrayMap("date_histogram", new ArrayMap("field", "time", "fixed_interval", interval)));
		}
		//Map 
		setBodyMap(new ArrayMap(
			"source", new ArrayMap("index", src),
			"dest", new ArrayMap("index", dest),
			"pivot", new ArrayMap("group_by", group_by, "aggregations", aggregations)));
		return this;
	}

}
