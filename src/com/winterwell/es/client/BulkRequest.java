package com.winterwell.es.client;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.winterwell.utils.TodoException;
import com.winterwell.utils.containers.ArrayMap;

/**
 * Make a bulk update / insert request from several other requests - see {@link #add(ESHttpRequest)}.
 * 
 * TODO add a streaming mode to this
 * 
 * @author Daniel
 * @testedby  BulkRequestBuilderTest}
 */
public class BulkRequest extends ESHttpRequest<BulkRequest,BulkResponse> {

	/**
	 * @return true if this is a no-op
	 */
	public boolean isEmpty() {
		return actions.isEmpty();
	}
	
	/**
	 * @deprecated TODO for large amounts of data, better to stream it out rather than build a big in-memory blob
	 * @return
	 */
	public BulkRequest openStream() {
		String sofar = getBodyJson();
		// chop off the end
		// open a url stream		
		return this;
	}
	
	public BulkRequest closeStream() {
		return this;
	}

	
	public List<ESHttpRequest> getActions() {
		return actions;
	}

	public BulkRequest(ESHttpClient hClient) {
		super(hClient, "_bulk");
		method = "POST";
	}

	List<ESHttpRequest> actions = new ArrayList();
	
	public BulkRequest add(ESHttpRequest request) {
		actions.add(request);		
		if (request.indices==null) {
			throw new IllegalArgumentException("No index set for "+request);
		}
		return this;
	}
		
	@Override
	public String getBodyJson() {
		StringBuilder srcJson = new StringBuilder();
		for(ESHttpRequest req : actions) {
			String op = req.bulkOpName;
			if (op==null) throw new TodoException(req);
			ArrayMap opMap = new ArrayMap(
					"_index", req.indices.get(0), "_type", req.type, "_id", req.id
			);
			if ( ! req.params.isEmpty()) {
				opMap.putAll(req.params);
			}
			Map actionObj = new ArrayMap(op, opMap);
			srcJson.append(gson().toJson(actionObj).trim()+"\n");
			srcJson.append(req.getBodyJson().trim()+"\n");
		}
		return srcJson.toString();
	}
	
}
