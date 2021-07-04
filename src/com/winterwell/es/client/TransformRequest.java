package com.winterwell.es.client;

import java.util.List;
import java.util.Map;

import com.winterwell.es.client.query.ESQueryBuilder;
import com.winterwell.utils.Utils;
import com.winterwell.utils.containers.ArrayMap;

/**
 * Transform indices to merge fields.
 * See https://www.elastic.co/guide/en/elasticsearch/reference/current/transforms.html
 * @testedby TransformRequestBuilderTest
 */
public class TransformRequest extends ESHttpRequest<TransformRequest, IESResponse> {
	
	/** called when a transform preview is requested */
	public TransformRequest(ESHttpClient esHttpClient) {
		super(esHttpClient, "_transform/_preview");
		setIndex(null); 
		method = "POST";
	}
	
	/** called when creating and starting a transform job request 
	 * 
	 * @param transform_job id + /_start or /_stop if relevant
	 * This identifier can contain lowercase alphanumeric characters (a-z and 0-9), 
	 * hyphens, and underscores. It must start and end with alphanumeric characters
	 * */
	public TransformRequest(ESHttpClient esHttpClient, String transform_job, String request) {
		super(esHttpClient, "_transform/"+transform_job);
//		// Check job name conforms
//		if ( ! Pattern.matches("[a-z0-9][a-z0-9_\\-]*", transform_job)) {
//			throw new IllegalArgumentException(transform_job);
//		}			
		setIndex(null); 
		method = request; 
	}
	
	/**
	 * Set the body of a transform request. Uses sum aggregation
	 * @param srcIndex index source (where the data comes from)
	 * @param destIndex index destination (where the aggregated data will go)
	 * @param terms the terms which you want to group by (i.e. what fields will get kept)
	 * @param interval (optional) if want to group by date histogram, specify interval format?? e.g. "1d"??
	 * This is locked to the `time` field ??do we want more flexibility
	 * @return this
	*/
	public TransformRequest setBody(String srcIndex, String destIndex, List<String> aggs, List<String> terms, String interval) {		
		ArrayMap aggregations = new ArrayMap("count", new ArrayMap("sum", new ArrayMap("field", "count")));
		for (String agg : aggs) {
			aggregations.put(agg, new ArrayMap("sum", new ArrayMap("field", agg)));
		}

		ArrayMap group_by = new ArrayMap();
		for (String term : terms) {
			//group_by.put(term, new ArrayMap("terms", new ArrayMap("field", term)));
			// for ES version >= 7.10.0, missing_bucket attribute supported to not ignore documents with null field
			group_by.put(term, new ArrayMap("terms", new ArrayMap("field", term, "missing_bucket", true)));
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

	/**
	 * Use this to filter the source, before transforming.
	 * @param query
	 */
	public void addQuery(ESQueryBuilder query) {
		assert body != null && body.containsKey("source") : "setBody first";
		Map source = (Map) body.get("source");
		source.put("query", query.toJson2());
	}
	
	/** Similar to setBody function, but with painless script support in case for ES version < 7.10.0 */
	public TransformRequest setBodyWithPainless(String srcIndex, String destIndex, List<String> aggs, List<String> terms, String interval) {		
		ArrayMap aggregations = new ArrayMap("count", new ArrayMap("sum", new ArrayMap("field", "count")));
		for (String agg : aggs) {
			aggregations.put(agg, new ArrayMap("sum", new ArrayMap("field", agg)));
		}

		ArrayMap group_by = new ArrayMap();
		String script;
		for (String term : terms) {
			script = "if (doc[\u0027"+term+"\u0027].size() == 0) {return \"\";}return doc[\u0027"+term+"\u0027].value;";
			group_by.put(term, new ArrayMap("terms", new ArrayMap("script", new ArrayMap(
					"source", script,
					"lang", "painless"))));
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
