package com.winterwell.es.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.winterwell.es.client.agg.Aggregation;
import com.winterwell.es.client.agg.Aggregations;
import com.winterwell.es.client.query.ESQueryBuilder;
import com.winterwell.es.client.query.ESQueryBuilders;
import com.winterwell.es.client.sort.Sort;
import com.winterwell.es.client.suggest.Suggester;
import com.winterwell.es.client.suggest.Suggesters;
import com.winterwell.utils.StrUtils;
import com.winterwell.utils.containers.ArrayMap;
import com.winterwell.utils.containers.Containers;
import com.winterwell.utils.log.Log;
import com.winterwell.utils.time.Dt;
import com.winterwell.utils.time.TUnit;

/**
 * @testedby SearchRequestBuilderTest
 * @see org.SearchRequest.action.search.SearchRequestBuilder
 * @author daniel
 *
 */
public class SearchRequest extends ESHttpRequest<SearchRequest,SearchResponse> {


	/**
	 * @param excluded Can use wildcards, e.g. "*.bloat"
	 * See http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-get.html#get-source-filtering
	 * @return 
	 */
	public SearchRequest setResultsSourceExclude(String... excluded) {
		params.put("_source_exclude", StrUtils.join(excluded, ","));
		return this;
	}
	/**
	 * @param included Can use wildcards, e.g. "*.bloat"
	 * See http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-get.html#get-source-filtering
	 * @return 
	 */
	public SearchRequest setResultsSourceInclude(String... included) {
		params.put("_source_include", StrUtils.join(included, ","));
		return this;
	}


	public SearchRequest(ESHttpClient hClient) {
		super(hClient, "_search");
		// what method is it?? probably post for the body 
	}


    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
	public SearchRequest setTypes(String... types) {
		assert types.length==1 : "TODO";
		setType(types[0]);
		return this;
	}


//	/**
//	 * See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-search-type.html
//	 * @param searchType
//	 * @return
//	 */
//	public SearchRequestBuilder setSearchType(SearchType searchType) {
//		return setSearchType(searchType.toString().toLowerCase());
//	}
	
	/**
	 * See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-search-type.html
	 * @param searchType e.g. "scan" (although that was deprecated in 2.1)
	 * @return
	 */
	public SearchRequest setSearchType(String searchType) {
		params.put("search_type", searchType);
		return this;
	}

	/**
	 * Best practice: Use this to set the query. / filter.
	 * @param qb Cannot be modified afterwards.
	 * @return
	 */
	public SearchRequest setQuery(ESQueryBuilder qb) {
		return setQuery(qb.toJson2());
	}
	/**
	 * Convenience method for building up AND queries.
	 * This will set the query if null, or combine with bool-query *must* if not null.
	 * 
	 * @see #setQuery(ESQueryBuilder)
	 * 
	 * @param qb
	 * @return 
	 */
	public SearchRequest addQuery(ESQueryBuilder qb) {
		Map query = (Map) body().get("query");
		if (query==null) {
			setQuery(qb);
			return this;
		}
		// Add to it
		// Is it a boolean?
//		String qtype = (String) Containers.first(query.keySet());
//		if (qtype != "bool") {
			ESQueryBuilder qand = ESQueryBuilders.must(query, qb);
			setQuery(qand.toJson2());
//		} else {
			// TODO merge!			
//		}
		return this;
	}
	

	public SearchRequest setQuery(Map queryJson) {
		body().put("query", queryJson);
		return this;
	}
	
	public SearchRequest setFrom(int i) {
		params.put("from", i);
		return this;
	}
	/**
	 * How many results to fetch. The default is 10.
	 * Important: If this is used in a scroll -- this is the *batch size*!
	 * @param n 
	 * @return this
	 */
	public SearchRequest setSize(int n) {
		params.put("size", n);
		return this;
	}

	public SearchRequest addSort(Sort sort) {
		// type Map (normal) | String (ScoreSort)
		List sorts = (List) body().get("sort");
		if (sorts==null) {
			sorts = new ArrayList();
			body().put("sort", sorts);
		}		
		sorts.add(sort.toJson2());
		return this;
	}
	
	/**
	 * Really just an aide-memoire for {@link #addSort(Sort)}.
	 * Differs in that being a `set` it will overwrite any existing value.
	 * @param sort
	 */
	public SearchRequest setSort(Sort sort) {
		List<Map> sorts = (List) body().get("sort");
		if (sorts!=null) {
			sorts.clear();
		}
		return addSort(sort);		

	}
	
	/**
	 * How long to keep scroll resources open between requests.
	 * NB: Scroll is typically used with setSort("_doc");
	 * 
	 * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
	 * @param keepAlive
	 * @see SearchScrollRequest
	 */
	public void setScroll(Dt keepAlive) {
		int s = (int) keepAlive.convertTo(TUnit.SECOND).getValue();
		params.put("scroll", s+"s");
	}


	/**
	 * See {@link Aggregations}
	 * Note: If you only want the aggregation results and not the documents, set size-0 with {@link #setSize(int)}.
	 * @return this
	 */
	public SearchRequest addAggregation(Aggregation dh) {
		// NB: This is copy pasta Aggregation.subAggregation()
		Map sorts = (Map) body().get("aggs");
		if (sorts==null) {
			sorts = new ArrayMap();
			body.put("aggs", sorts);
		}
		// e.g.      "grades_stats" : { "stats" : { "field" : "grade" } }
		Object noOld = sorts.put(dh.name, dh);
		// safety check
		if (noOld != null) {
			if (noOld==dh) {
				Log.w("search", "2x aggregation "+dh.name);
			} else {
				throw new IllegalStateException("Duplicate named aggregations: "+dh.name+" "+noOld+" vs "+dh);
			}
		}
		return this;		
	}
	
	/**
	 * See {@link Suggesters}
	 * @return this
	 */
	public SearchRequest addSuggester(Suggester suggester) {
		// NB: This is copy pasta Aggregation.subAggregation()
		Map sorts = (Map) body().get("suggest");
		if (sorts==null) {
			sorts = new ArrayMap();
			body.put("suggest", sorts);
		}
		sorts.put(suggester.name, suggester); //.toJson2()); // TODO support late json conversion
		// but caused a bug -- why is this behaving differently to Aggregation??
		return this;		
	}
	
	/**
	 * 
	 * @return Can be null (unset => 10)
	 */
	public Integer getSize() {
		Number n = (Number) params.get("size");
		return n==null? null : n.intValue();
	}
	
	/**
	 * See https://www.elastic.co/guide/en/app-search/7.9/search-fields-weights.html
	 * @param fields
	 */
	public void setSearchFields(List<String> fields) {
		Map<String, Object> b = body();
		ArrayMap fs = new ArrayMap();
		for (String f : fields) {
			fs.put(f, Collections.EMPTY_MAP);
		}
		b.put("search_fields", fs);
	}
	
	/**
	 * See https://www.elastic.co/guide/en/app-search/7.9/search-fields-weights.html
	 * Weights are from 10 (most relevant) to 1 (least relevant).
	 */
	public void setSearchFields(Map<String,Number> field2weight) {
		Map<String, Object> b = body();
		ArrayMap fs = new ArrayMap();
		for (String f : field2weight.keySet()) {
			Number w = field2weight.get(f);
			int wi = w.intValue();
			if (wi < 1 || wi > 10) throw new IllegalArgumentException("Weight must be in [1,10] "+field2weight);
			fs.put(f, new ArrayMap("weight", wi));
		}
		b.put("search_fields", fs);
	}
	
}
