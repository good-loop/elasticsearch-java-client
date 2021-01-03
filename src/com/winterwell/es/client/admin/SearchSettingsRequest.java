package com.winterwell.es.client.admin;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.winterwell.es.ESType;
import com.winterwell.es.client.ESHttpClient;
import com.winterwell.es.client.ESHttpRequest;
import com.winterwell.es.client.IESResponse;
import com.winterwell.es.client.SearchRequest;
import com.winterwell.utils.containers.ArrayMap;

/**
 * 
 * @deprecated Status: Not working! 
 * The ES docs are pretty poor on how this should be setup. Parking for now.
 * 
 * Show or set settings like which fields to query for an index.
 * 
 * https://www.elastic.co/guide/en/app-search/7.9/search-settings.html
 * @author daniel
 * @testedby {@link SearchSettingsRequestTest}
 */
public class SearchSettingsRequest extends ESHttpRequest<SearchSettingsRequest,IESResponse> {

	public SearchSettingsRequest(ESHttpClient hClient, String idx) {
		super(hClient, "search_settings");
		setIndex(idx);
		method = "GET"; // PUT if we set anything
	}

	public SearchSettingsRequest setSearchFields(List<String> fields) {
		setSearchFields2(body(), fields);
		method = "PUT";
		return this;
	}
	public SearchSettingsRequest setSearchFieldWeights(Map<String,Number> field2weight) {
		setSearchFieldWeights2(body(), field2weight);
		method = "PUT";
		return this;
	}

	/**
	 * @deprecated For internal use
	 * @param requestBody
	 * @param fields
	 */
	public static void setSearchFields2(Map<String, Object> requestBody, List<String> fields) {
		ArrayMap fs = new ArrayMap();
		for (String f : fields) {
			fs.put(f, Collections.EMPTY_MAP);
		}
		requestBody.put("search_fields", fs);
	}
		
		/**
		 * @deprecated For internal use
		 * See https://www.elastic.co/guide/en/app-search/7.9/search-fields-weights.html
		 * Weights are from 10 (most relevant) to 1 (least relevant).
		 */
	public static void setSearchFieldWeights2(Map<String, Object> requestBody, Map<String,Number> field2weight) {
		ArrayMap fs = new ArrayMap();
		for (String f : field2weight.keySet()) {
			Number w = field2weight.get(f);
			int wi = w.intValue();
			if (wi < 1 || wi > 10) throw new IllegalArgumentException("Weight must be in [1,10] "+field2weight);
			fs.put(f, new ArrayMap("weight", wi));
		}
		requestBody.put("search_fields", fs);
	}

}
