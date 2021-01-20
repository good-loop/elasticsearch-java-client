/**
 * 
 */
package com.winterwell.es.client.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.winterwell.es.client.query.ESQueryBuilder;
import com.winterwell.utils.containers.ArrayMap;
import com.winterwell.utils.time.Dt;
import com.winterwell.utils.time.TUnit;
import com.winterwell.utils.time.Time;
/**
 * Builder methods for making {@link Aggregation}s
 * @author daniel
 *
 */
public class Aggregations {

	/**
	 * Default to one day interval
	 */
	public static Aggregation dateHistogram(String name, String field) {
		return dateHistogram(name, field, TUnit.DAY.dt);
	}
	
	public static Aggregation filtered(String name, ESQueryBuilder filter, Aggregation agg) {
		Aggregation fagg = new Aggregation(name, null, null);
		fagg.map.put("filter", filter.toJson2());
		fagg.subAggregation(agg);
		return fagg;
	}

	/**
	 * https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html
	 * @param name
	 * @param interval e.g. day / month / hour
	 * @return
	 */
	public static Aggregation dateHistogram(String name, String field, Dt interval) {
		// TODO properly implement https://www.elastic.co/guide/en/elasticsearch/reference/7.0/search-aggregations-bucket-datehistogram-aggregation.html
		String _interval = interval.getUnit().toString().toLowerCase();
		if (interval.getValue() != 1.0) {
			_interval = ((int)interval.getValue())+_interval; // FIXME do fractions work?!
		}
		// TODO s/interval/calendar_interval or fixed_interval/
		return new Aggregation(name, "date_histogram", field).put("interval", _interval);
	}

	/**
	 * ??TODO accuracy and coverage??
	 * Stats on a numeric field
	 * @param name
	 * @param field This MUST be numeric
	 * @return
	 */
	public static Aggregation stats(String name, String field) {
		return new Aggregation(name, "stats", field);
	}

	/**
	 * https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-sum-aggregation.html
	 * @param name
	 * @param field This MUST be numeric
	 * @return
	 */
	public static Aggregation sum(String name, String field) {
		return new Aggregation(name, "sum", field);
	}
	
	/**
	 * document count by field=value,
	 * If you want to sum a numerical value from the documents, use this with a stats() subAggregation
	 * @param name Your name for this aggregation
	 * @param field usually a keyword field
	 * @return
	 */
	public static Aggregation terms(String name, String field) {
		return new Aggregation(name, "terms", field);
	}
	
	/**
	 * ??How does this differ from {@link #terms(String, String)}??
	 * @param name
	 * @param field
	 * @return
	 */
	public static Aggregation significantTerms(String name, String field) {
		return new Aggregation(name, "significant_terms", field);
	}

	/**
	 * Can ES output be simplified??
	 * @param aggResult
	 * @return
	 */
	public static Map<String,Object> simplify(Map<String,Object> aggResult) {
//		Map<String, Object> simpler = new ArrayMap();
//		// from {buckets -> {key, doc_count by_time -> sub-agg}} 
//		List<Map> buckets = (List) aggResult.get("buckets");
//		if (buckets==null) {
//			
//		}
//		for (Map<String,Object> bucket : buckets) {
//			String key = (String) bucket.get("key_as_string");
//			Object key = bucket.get("key");
//			// does it have a sub-aggregation in it?
//			boolean subAgg = false;
//			for(Map.Entry me : bucket.entrySet()) {
//				// which is a value that's a map and has buckets in 
//				Object v = me.getValue();
//				if (v instanceof Map && ((Map) v).containsKey("buckets")) {
//					Map vs = simplify((Map)v);
//					simpler.put(""+key, vs);
//					subAgg = true;
//					break; // that's all for this bucket
//				}
//			}
//			if ( ! subAgg) {
//				continue;
//			}
//		}
		// to {key -> sub-key -> stats
		
				
		return aggResult;
	}

	/**
	 * See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html
	 * @param name
	 * @param field
	 * @param times The start/end markers for the buckets. You can start or end the list with null,
	 * to specify an open-ended start/end.
	 * 	The results include the `from` value and excludes the `to` value for each range.
	 * @return
	 */
	public static Aggregation dateRange(String name, String field, List<Time> times) {
		Aggregation agg = new Aggregation(name, "date_range", field);
		List<Map> ranges = new ArrayList();
		// "to": "now-10M/M" <-- interesting format: "< now minus 10 months, rounded down to the start of the month"		
		for(int i=1; i<times.size(); i++) {
			Time prev = times.get(i-1);
			Time time = times.get(i);
			ranges.add(new ArrayMap(
					"from", prev==null? null : prev.getTime(),
					"to", time==null? null : time.getTime()
					));
			prev=time;
		}
		agg.put("ranges", ranges);
		return agg;
	}	
}
