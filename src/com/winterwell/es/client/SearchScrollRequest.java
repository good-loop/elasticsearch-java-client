package com.winterwell.es.client;

import com.winterwell.utils.time.Dt;
import com.winterwell.utils.time.TUnit;

/**
 * 
 * https://www.elastic.co/guide/en/elasticsearch/reference/6.2/search-request-scroll.html
 * 
 * @see org.SearchScrollRequest.action.search.SearchScrollRequestBuilder
 * @author daniel
 *
 */
public class SearchScrollRequest extends ESHttpRequest<SearchScrollRequest, SearchResponse> {

	/**
	 * 
	 * @param esHttpClient
	 * @param scrollId
	 * @param scrollWindow How long to keep the scroll open for until the next request. Can be null (in which case this request will close the scroll).
	 * Suggested value: 1 minute
	 */
	public SearchScrollRequest(ESHttpClient esHttpClient, String scrollId, Dt scrollWindow) {
		super(esHttpClient, "_search/scroll");
		params.put("scroll_id", scrollId);
		// You must keep setting a scroll window to keep the scroll alive.
		// This is such a gotcha, that we'll make the user set it here.
		setScroll(scrollWindow);
		method = "POST";
		setIndices(); // no index - it comes from the scroll id
	}
	
	@Override
	protected StringBuilder getUrl(String server) {
		// no indices
		StringBuilder url = new StringBuilder(server);
		url.append("/"+endpoint);				
		return url;
	}
	
	/**
	 * The size parameter controls the number of results per shard, not per request, so a size of 10 which hits 5 shards 
	 * will return a maximum of 50 results per scroll request.
	 * @param n
	 */
	public SearchScrollRequest setSize(int n) {
		params.put("size", n);
		return this;
	}

	
	@Override
	public SearchScrollRequest setIndex(String idx) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public SearchScrollRequest setType(String type) {
		throw new UnsupportedOperationException();
	}

	public void setScroll(Dt keepAlive) {
		int s = (int) keepAlive.convertTo(TUnit.SECOND).getValue();
		params.put("scroll", s+"s");
	}
	
}