package com.winterwell.es.client;

import java.util.ArrayList;
import java.util.List;

import com.winterwell.utils.Utils;
import com.winterwell.utils.containers.ArrayMap;

/**
 * Transform indices to merge fields.
 * See https://www.elastic.co/guide/en/elasticsearch/reference/7.9/transforms.html
 * @testedby TransformRequestBuilderTest
 */
public class TransformRequestBuilder extends ESHttpRequest<TransformRequestBuilder, IESResponse> {
	
	/** called when a transform preview is requested */
	public TransformRequestBuilder(ESHttpClient esHttpClient) {
		super(esHttpClient, "_transform/_preview");
		setIndex(null); 
		method = "POST";
	}
	
	/** called when creating and starting a transform job request */
	public TransformRequestBuilder(ESHttpClient esHttpClient, String transform_job, String request) {
		super(esHttpClient, "_transform/"+transform_job);
		setIndex(null); 
		method = request; 
	}
	
	/**
	 * Set the body of a transform request. Uses count aggregations (what about sum??)
	 * @param srcIndex index source (where the data comes from)
	 * @param destIndex index destination (where the aggregated data will go)
	 * @param terms the terms which you want to group by (i.e. what fields will get kept)
	 * @param interval (optional) if want to group by date histogram, specify interval format?? e.g. "1d"??
	 * This is locked to the `time` field ??do we want more flexibility
	 * @return this
	*/
	public TransformRequestBuilder setBody(String srcIndex, String destIndex, List<String> terms, String interval) {		
		// counts ??do we want sums in places??
		ArrayMap aggregations = new ArrayMap("count", new ArrayMap("value_count", new ArrayMap("field", "count")));

		ArrayMap group_by = new ArrayMap();
		for (String term : terms) {
			group_by.put(term, new ArrayMap("terms", new ArrayMap("field", term)));
		}
		if ( ! Utils.isBlank(interval)) {
			group_by.put("time", new ArrayMap("date_histogram", new ArrayMap(
					"field", "time", 
					"fixed_interval", interval
					)));
		}
		//Map 
		setBodyMap(new ArrayMap(
			"source", new ArrayMap("index", srcIndex),
			"dest", new ArrayMap("index", destIndex),
			"pivot", new ArrayMap(
					"group_by", group_by, 
					"aggregations", aggregations
					)
			));
		return this;
	}

}
