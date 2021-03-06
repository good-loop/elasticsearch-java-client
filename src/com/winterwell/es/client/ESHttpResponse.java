package com.winterwell.es.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.winterwell.es.client.agg.AggregationResults;
import com.winterwell.es.fail.ESBulkException;
import com.winterwell.es.fail.ESException;
import com.winterwell.gson.Gson;
import com.winterwell.gson.GsonBuilder;
import com.winterwell.utils.Dep;
import com.winterwell.utils.StrUtils;
import com.winterwell.utils.Utils;
import com.winterwell.utils.containers.Containers;
import com.winterwell.utils.log.Log;
import com.winterwell.utils.web.IHasJson;
import com.winterwell.web.ajax.JThing;

/**
 * Imitates {@link GetResponse}
 * @author daniel
 *
 */
public class ESHttpResponse implements IESResponse, SearchResponse, BulkResponse, GetResponse, 
// allow response to be sent as json over http 
IHasJson 
{

	
	@Override
	public Long getVersion() {
		Map<String, Object> jmap = getJsonMap();
		Number v = (Number) jmap.get("_version");
		return v==null? null : v.longValue();
	}
	
	private final String json;
	private final RuntimeException error;
	/**
	 * Allow for null
	 */
	private final transient ESHttpRequest req;
	private Map parsed;
	private boolean sourceOnly;

	/* (non-Javadoc)
	 * @see com.winterwell.es.client.IESResponse#toString()
	 */
	@Override
	public String toString() {
		return "ESHttpResponse["+StrUtils.ellipsize(Utils.or(json, ""+error), 120)+"]";
	}
	
	/**
	 * For use by wrapper sub-classes
	 * @param response
	 */
	protected ESHttpResponse(ESHttpResponse response) {
		this(response.req, response.json, response.error);
	}
	
	public ESHttpResponse(ESHttpRequest req, String json) {
		this(req, json, null);
	}

	/**
	 * 
	 * @param req
	 * @param ex Should we standardise on {@link ESException}??
	 */
	public ESHttpResponse(ESHttpRequest req, RuntimeException ex) {
		this(req, null, ex);
	}

	ESHttpResponse(ESHttpRequest req, String json, RuntimeException ex) {
		this.req = req;
		this.json = json;
		
//		// HACK - ESv7 doesnt throw errors from bulk requests 
//		if (ex==null && json!=null && req instanceof BulkRequestBuilder) {
//			Map<String, Object> jobj = getParsedJson();
//			Object errors = jobj.get("errors");
//			if (Utils.yes(errors)) {
//				List<Map> items = (List) jobj.get("items");
//				List<Object> itemErrors = Containers.filterNulls(Containers.apply(items, i -> i.get("error")));
//				
//				ex = new ESBulkException(itemExs);
//				Printer.out(items);
//			}
//		}
		this.error = ex;
		// source only?
		if (req instanceof GetRequest && ((GetRequest) req).sourceOnly) {			
			sourceOnly = true;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see com.winterwell.es.client.IESResponse#isSuccess()
	 */
	@Override
	public boolean isSuccess() {
		return error==null;
	}

	@Override
	public Map<String, Object> getSourceAsMap() {
		check();
		Map<String, Object> map = getParsedJson();
		// is it just the source?
		if (sourceOnly) {
			return map;
		}
		Object source = map.get("_source");
		return (Map) source;
	}
	
	public Object getFromResultFields(String input) {
		Map<String, Object> map = getParsedJson();
		// is it just the source?
		Object get = map.get("get");
		Object fields = ((Map) get).get("fields");
		return ((Map) fields).get(input);
	}
	
	/**
	 * @return Could contain POJOs depending on gson setup.
	 *  
	 * @see #getJsonMap() which uses a "plain gson" for Map objects 
	 */
	public Map<String, Object> getParsedJson() {
		if (parsed!=null) return parsed;		
		parsed = gson().fromJson(json, Map.class);
		return parsed;
	}
	
	/**
	 * Uses a "plain" (inflexible) Gson, so nothing gets converted into "fancy" POJOs.
	 * @return
	 */
	public Map<String, Object> getJsonMap() {
		Map map = plainGson().fromJson(json, Map.class);
		return map;
	}
	
	
	
	private Gson plainGson() {
		Gson gb = new GsonBuilder()
						.setClassProperty(null)
						.create();
		return gb;
	}

	/**
	 * The raw json as returned by ES.
	 */
	public String getJson() {
		return json;
	}
	
	
	
	private Gson gson() {
		// req should never be null -- unless its been serialised and back		
		if (req==null || req.hClient==null) {
			// A deserialised response (hence with no request).
			// ...fallback to Dep for Gson
			if (Dep.has(Gson.class)) {
				return Dep.get(Gson.class);
			}
			return new Gson(); // fallback to default
		}
		return req.hClient.config.getGson();				
	}

	/* (non-Javadoc)
	 * @see com.winterwell.es.client.IESResponse#isAcknowledged()
	 */
	@Override
	public boolean isAcknowledged() {
		return isSuccess();
	}

	@Override
	public List getField(String name) {
		Map<String, Object> map = getFieldsFromGet();
		Object output = map.get(name);
		return (List) output;
	}
	
	@Override
	public String getSourceAsString() {
		check();
		// is it just the source?
		if (sourceOnly) {
			return json;
		}
		Map<String, Object> map = getParsedJson();
		Object source = map.get("_source");
		return gson().toJson(source);
	}

	// From BulkResponse
	@Override
	public boolean hasErrors() {		
		if ( ! isSuccess()) {			
			return true;
		}
		Map<String, Object> map = getParsedJson();
		Object fails = map.get("errors"); // NB: boolean in ESv7, was the errors in ESv5		
		if (Utils.yes(fails)) {
			return true;
		}
		return false;
	}
	
	/**
	 * error or null
	 * 
	 * TODO handle bulk-request errors, which are different 
	 * @see #getBulkErrors()  
	 */
	public RuntimeException getError() {
		// HACK! unreliable if deserialising cos req is transient
		if (req instanceof BulkRequest && error==null) {
			return getBulkErrors();
		}
		return error;
	}
	
	/**
	 * TODO handle bulk-request errors nicely
	 * @return
	 */
	RuntimeException getBulkErrors() {
		List<Exception> errors = new ArrayList<>();
		Map<String, Object> parsedJson = getParsedJson();
		List<Map<String, Map<String, Object>>> items = (List) parsedJson.get("items");
		if (items == null) return null;
		for(Map<String, Map<String, Object>> item : items) {
			for (Map.Entry<String, Map<String, Object>> entry : item.entrySet()) {
				Map<String, Object> values = entry.getValue();
				Map err = (Map) values.get("error");
				if (err == null) continue;
				String errs = err.toString();
				ESException ex = new ESException((String)err.get("reason"));				
				errors.add(ex);									
			}
		}
		if ( ! errors.isEmpty()) {
			return new ESBulkException(errors);
		}		
		return null;
	}
	
	
	@Override
	public ESHttpResponse check() {
		if (error!=null) {
			Log.d("fail.check", "throw from "+req);
			throw error;
		}
		return this;
	}
	

	@Override
	public List<Map> getHits() {
		if ( ! isSuccess()) throw error;
		Map<String, Object> map = getParsedJson();
		Map hits = (Map) map.get("hits");
		Object hitsList = hits.get("hits");
		return (List<Map>) hitsList;
	}

	@Override
	public List<Map<String, Object>> getSearchResults() {
		if ( ! isSuccess()) throw error;
		Map<String, Object> map = getJsonMap();
		Map hits = (Map) map.get("hits");
		List<Map<String,Object>> hitsList = (List) hits.get("hits");
		List results = Containers.apply(hitsList, hit -> hit.get("_source"));
		return results;		
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Uses {@link #gson()} for the convertor.
	 */
	@Override
	public <X> List<X> getSearchResults(Class<? extends X> klass) {
		check();
		Map<String, Object> jobj = getJsonMap();
		List<Map> hits = (List<Map>) ((Map)jobj.get("hits")).get("hits");
		List<X> results = Containers.apply(hits, map -> gson().convert((Map)map.get("_source"), klass));
		return results;
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Uses {@link JThing} for the convertor.
	 */
	@Override
	public <X> List<ESHit<X>> getHits(Class<? extends X> type) {
		check();
		Map<String, Object> jobj = getJsonMap();
		List<Map> hits = (List<Map>) ((Map)jobj.get("hits")).get("hits");
		List<ESHit<X>> results = Containers.apply(hits, 
				map -> gson().convert(map, ESHit.class).setType(type)
			);
		return results;
	}

	
	@Override
	public Map getAggregations() {
		if ( ! isSuccess()) throw error;
		Map<String, Object> map = getParsedJson();
		Map hits = (Map) map.get("aggregations");
		return hits;
	}
	
	@Override
	public AggregationResults getAggregationResults(String aggName) {
		Map rs = (Map) getAggregations().get(aggName);
		return new AggregationResults(aggName, rs);
	}

	/**
	 * @return Never null - can throw exceptions
	 */
	@Override
	public long getTotal() throws IllegalArgumentException {
		if ( ! isSuccess()) throw error;
		Map<String, Object> map = getJsonMap();
		Map hits = (Map) map.get("hits");
		if (hits == null){
			throw new IllegalArgumentException("hits field cannot be null");
		}
		// c.f. https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
		Object hitTotal = hits.get("total");
		if (hitTotal == null){
			throw new IllegalArgumentException("hitTotal field cannot be null "+hits);
		}
		// ESv6
		if (hitTotal instanceof Number) {
			return ((Number) hitTotal).longValue();
		}
		// ESv7
		Number hitTotalValue = (Number) ((Map)hitTotal).get("value");
		return hitTotalValue.longValue();
	}

	
	@Override
	public Map getFacets() {
		if ( ! isSuccess()) throw error;
		Map<String, Object> map = getParsedJson();
		Object hits = map.get("facets");
		return (Map) hits;
	}

	@Override
	public Map getFieldsFromGet() {
		if (error!=null) throw error;
		Map<String, Object> map = getParsedJson();
		Map<String, Object> get = (Map<String, Object>) map.get("get");
		Map<String, Object> hits = (Map<String, Object>) (Map<String, Object>) get.get("fields");
		return hits;
	}
	
	@Override
	public String getScrollId() {
		if ( ! isSuccess()) throw error;
		Map<String, Object> map = getParsedJson();
		Object sid = map.get("_scroll_id");
		return (String) sid;
	}

	@Override
	public List<Map> getSuggesterHits(String name) {
		if ( ! isSuccess()) throw error;
		Map<String, Object> map = getParsedJson();
		Map suggesters = (Map) map.get("suggest");
		List<Map> res = (List<Map>) suggesters.get(name);
		//  Do num -> result -> options -> num -> _source to get a doc
		List hits = Containers.flatten(Containers.apply(res, r -> r.get("options")));
		return hits;
	}

	@Override
	public Object toJson2() throws UnsupportedOperationException {
		return Containers.objectAsMap(this);
	}

	
}
