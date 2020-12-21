package com.winterwell.es.client.admin;

import com.winterwell.es.client.ESHttpClient;
import com.winterwell.es.client.ESHttpRequest;
import com.winterwell.es.client.ESHttpResponse;
import com.winterwell.es.client.IESResponse;
import com.winterwell.utils.TodoException;
import com.winterwell.utils.Utils;
import com.winterwell.utils.containers.ArrayMap;
import com.winterwell.web.FakeBrowser;

/**
 * TODO
 * @author daniel
 *
 */
public class ClusterOverridReadOnlyRequest extends ESHttpRequest<ClusterOverridReadOnlyRequest,IESResponse> {

	public ClusterOverridReadOnlyRequest(ESHttpClient hClient) {
		super(hClient, "_cluster/settings");
		method = "PUT";
		setBodyMap(new ArrayMap(
			"transient", new ArrayMap("cluster.routing.allocation.disk.threshold_enabled", false)		
		));
		setIndices(); // Hack: no index, not "_all"  
/* for use on the command line:
 
curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_cluster/settings -d '{ "transient": { "cluster.routing.allocation.disk.threshold_enabled": false } }'
curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_all/_settings -d '{"index.blocks.read_only_allow_delete": null}'
*/
	}
	
	@Override
	protected ESHttpResponse doExecute(ESHttpClient esjc) {
//		curl -X PUT "localhost:9200/_cluster/settings" -H 'Content-Type: application/json' -d'
//		{
//		  "transient": {
//		    "cluster.routing.allocation.disk.watermark.low": "30mb",
//		    "cluster.routing.allocation.disk.watermark.high": "20mb",
//		    "cluster.routing.allocation.disk.watermark.flood_stage": "10mb",
//		    "cluster.info.update.interval": "1m"
//		  }
//		}
//		'
		ESHttpResponse res = super.doExecute(esjc);
		res.check();
		
		// 2nd call
		FakeBrowser fb = fb(esjc);
		fb.setRequestMethod("PUT");
		String server = Utils.getRandomMember(esjc.getServers());
		String url = getUrl(server).toString();
		// HACK
		url = url.replace("/_cluster/settings", "/_all/_settings"); // why does settings sometimes have "_"? Dunno.
		String body2 = "{\"index.blocks.read_only_allow_delete\": null}";
		String got = fb.post(url, FakeBrowser.CONTENT_TYPE_JSON, body2);
		ESHttpResponse res2 = new ESHttpResponse(this, got);
		//
		return res2;
	}

}
