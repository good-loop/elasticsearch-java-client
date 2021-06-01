package com.winterwell.es.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.winterwell.es.ESPath;
import com.winterwell.utils.StrUtils;
import com.winterwell.utils.containers.ArrayMap;
import com.winterwell.utils.containers.Containers;

/**
 * 
 * See https://www.elastic.co/guide/en/elasticsearch/guide/current/_retrieving_multiple_documents.html 
 * 
 * result:
 * docs: [{found: Boolean, _index, _type, _id, _version, _source}]
 * 
 * @author daniel
 *
 */
public class MultiGetRequest extends ESHttpRequest<MultiGetRequest, IESResponse> {
	
	boolean sourceOnly;
	private List<ESPath> docs = new ArrayList();

	public MultiGetRequest(ESHttpClient hClient) {
		super(hClient, "_mget");
		method = "POST";
		// make the body map now - otherwise doExecute() won't call #body() when needed
		body();
	}
	
	@Override
	public MultiGetRequest setId(String id) {
		throw new UnsupportedOperationException("Use addDoc instead");
	}
	
	/**
	 * 
	 * @param doc id is necessary; type and index can be specified in doc OR on the request (i.e. once for all docs).
	 */
	public MultiGetRequest addDoc(ESPath doc) {
		docs.add(doc);
		return this;
	}
	
	
	@Override
	public String getBodyJson() {
		// lazy creation of docs
		Map<String, Object> _body = body();				
		List<Map> _docs = Containers.apply(docs, doc -> {
			ArrayMap m = new ArrayMap("_id", doc.id);
			if (getIndices()==null) m.put("_index", doc.indices[0]);
			if (type==null) m.put("_type", doc.type);
			return m;
		});
		_body.put("docs", _docs);
		// then as normal
		return super.getBodyJson();
	}

	/**
	 * @param excluded Can use wildcards, e.g. "*.bloat"
	 * See http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-get.html#get-source-filtering
	 * @return 
	 */
	public MultiGetRequest setResultsSourceExclude(String... excluded) {
		params.put("_source_excludes", StrUtils.join(excluded, ","));
		return this;
	}
	/**
	 * @param included Can use wildcards, e.g. "*.bloat"
	 * See http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-get.html#get-source-filtering
	 * @return 
	 */
	public MultiGetRequest setResultsSourceInclude(String... included) {
		params.put("_source_includes", StrUtils.join(included, ","));
		return this;
	}

	
	 /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     * @param preference e.g. "_local"
     */
    public MultiGetRequest setPreference(String preference) {
    	params.put("preference", preference);
        return this;
    }
    
    @Override
    protected StringBuilder getUrl(String server) {
    	// see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html
		return super.getUrl(server);
    }

    /**
     * If true, only return the item _source json, without the surrounding score and other metadata.
     * @return 
     */
	public MultiGetRequest setSourceOnly(boolean b) {
		sourceOnly = b;
		return this;
	}


}
