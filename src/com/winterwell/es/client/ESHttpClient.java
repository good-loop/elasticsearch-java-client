package com.winterwell.es.client;

import java.io.Flushable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.winterwell.es.ESPath;
import com.winterwell.es.client.admin.IndicesAdminClient;
import com.winterwell.es.client.admin.StatsRequest;
import com.winterwell.gson.JsonElement;
import com.winterwell.gson.JsonObject;
import com.winterwell.gson.JsonParser;
import com.winterwell.utils.Dep;
import com.winterwell.utils.Utils;
import com.winterwell.utils.log.Log;
import com.winterwell.utils.time.TUnit;
import com.winterwell.utils.web.SimpleJson;
import com.winterwell.web.ConfigException;
import com.winterwell.web.FakeBrowser;

/**
 * This object is thread safe. 
 * You can choose whether to get futures (via {@link #executeThreaded(ESHttpRequest)}), or just normal execute-in-the-current-thread
 * behaviour (via {@link #execute(ESHttpRequest)}).
 * @author daniel
 *
 */
public class ESHttpClient implements Flushable {

	/**
	 * 
	 * @return e.g. "7.10.0"
	 */
	public String getESVersion() {
		FakeBrowser fb = new FakeBrowser();
		fb.setTimeOut(config.esRequestTimeout); // 1 minute timeout
		fb.setDebug(debug);
//		fb.setRequestHeader("Content-Type", "application/json");
		String json = fb.getPage(config.getESUrl());
		JsonElement jelement = new JsonParser().parse(json);
	    JsonObject  jobject = jelement.getAsJsonObject();
	    jobject = jobject.getAsJsonObject("version");
	    return jobject.get("number").getAsString();
	}

	public ESConfig getConfig() {
		return config;
	}
	
	/**
	 * You can optionally request a future.
	 */
	private static final ListeningExecutorService threads 
							= MoreExecutors.listeningDecorator(
									Executors.newFixedThreadPool(20));

	
	

	/**
	 * Call ES to check the connection is alive and well.
	 * @throws ConfigException
	 */
	public void checkConnection() {
		// try 3 times
		Throwable cause = null;
		for(int i=0; i<3; i++) {
			try {
				StatsRequest listreq = admin().indices().listIndices();
				ESHttpResponse listresponse = listreq.get().check();
				// all fine :)
				return;
			} catch(Throwable ex) {
				cause = ex;
				Log.w("ES", ex);
				Utils.sleep(100 + i*500);
			}
		}
		// fail!
		throw new ConfigException("Failed with settings: "+config, "ES", cause);
	}

	List<String> servers;

	public List<String> getServers() {
		return servers;
	}

	private boolean closed;


	final ESConfig config;


	@Deprecated // set on requests
	public static boolean debug;

	public void setServer(String server) {
		this.servers = Collections.singletonList(server);
	}
	
	/**
	 * @warning This relies on Dep.get(ESConfig.class) 
	 */
	public ESHttpClient() {
		this(Dep.get(ESConfig.class));
	}
	
	public ESHttpClient(ESConfig config) {
		this.config = config;
		if (config==null) throw new NullPointerException("null config for ES");
		String s = config.esUrl;		
		servers = Arrays.asList(s);
	}

	/**
	 * @deprecated Equivalent to {@link #admin().indices()}
	 */
	public IndicesAdminClient getIndicesAdminClient() {
		return admin().indices();
	}

	public AdminClient admin() {
		return new AdminClient(this);
	}
	
	/**
	 * Use a thread-pool to call async -- immediate response, future result.
	 * @param req
	 * @return
	 */
	ListenableFuture<ESHttpResponse> executeThreaded(final ESHttpRequest req) {
		CallES call = new CallES(req);
		ListenableFuture<ESHttpResponse> future = getThreads().submit(call);
		return future;
	}
	
	public static ListeningExecutorService getThreads() {
		return threads;
	}
	
	
	/**
	 * Pass the call across threads. This will "preserve" stacktrace across threads for easier debugging.
	 */
	class CallES implements Callable<ESHttpResponse> {
		
		@Override
		public String toString() {
			return "CallES["+req+"]";
		}
		private ESHttpRequest req;
		StackTraceElement[] trace;
		
		CallES(ESHttpRequest req) {		
			assert req != null;
			this.req=req;
			// Keep the caller's stacktrace for reporting on errors
			if (debug) {
				try {
					throw new Exception();
				} catch (Exception e) {
					trace = e.getStackTrace();
				}
			}
		}
		
		@Override
		public ESHttpResponse call() throws Exception {
			try {
				Thread.currentThread().setName("ESHttpClient (threaded): "+req);				
				assert req.retries+1 >= 1;
				ESHttpResponse r = null;
				for(int t=0; t<req.retries+1; t++) {
					r = req.doExecute(ESHttpClient.this);
					// success?
					if (r.getError()==null) return r;
					// pause before a retry to allow whatever the problem was to clear
					// but first retry is near instant
					Utils.sleep(5 + t*t*1000);
				}
				// fail
				if (trace!=null) {				
					r.getError().setStackTrace(trace);
				}
				return r;
			} catch(Throwable ex) {
				// This shouldn't generate errors -- but if it does, don't let them just get lost!
				Log.e("ES", ex);
				throw Utils.runtime(ex);
			}
		}			
	}	

	@Override
	public String toString() {
		return "ESHttpClient[servers=" + servers + "]";
	}

	/**
	 * @deprecated go typeless
	 * Prepare to index (aka store or insert) a document!
	 * @param index
	 * @param type
	 * @param id
	 * @return an IndexRequestBuilder Typical usage: call setSource(), then get()
	 */
	public IndexRequest prepareIndex(String index, String type, String id) {
		return prepareIndex(new ESPath(index, type, id));
	}
	
	/**
	 * Prepare to index (aka store or insert) a document!
	 * @param index
	 * @param id
	 * @return an IndexRequestBuilder Typical usage: call setBodyDoc(), then get()
	 */
	public IndexRequest prepareIndex(String index, String id) {
		return prepareIndex(new ESPath(index, id));
	}

	public DeleteRequest prepareDelete(String esIndex, String esType, String id) {
		com.winterwell.es.client.DeleteRequest drb = new DeleteRequest(this);
		drb.setIgnoreError404(true);
		drb.setIndex(esIndex).setType(esType).setId(id);
		return drb;
	}
	
	public DeleteRequest prepareDelete(ESPath path) {
		return prepareDelete(path.index(), path.type, path.id);
	}

	/**
	 * Convenience for using {@link GetRequest} to get a document (with no routing)
	 * @return source-as-map, or null if not found
	 */
	public Map<String, Object> get(String index, String type, String id) {
		GetRequest gr = new GetRequest(this);
		gr.setIndex(index).setType(type).setId(id);
		gr.setSourceOnly(true);
		GetResponse r = gr.get();
		if ( ! r.isSuccess()) return null;
		return r.getSourceAsMap();
	}
	
	/**
	 * 
	 * @param path
	 * @return source-as-map, or null if not found
	 */
	public Map<String, Object> get(ESPath path) {
		return get(path.index(), path.type, path.id);
	}

	public <X> X get(String index, String type, String id, Class<X> class1) {
		GetRequest gr = new GetRequest(this);
		gr.setIndex(index).setType(type).setId(id);
		gr.setSourceOnly(true);
		GetResponse r = gr.get();
		if ( ! r.isSuccess()) return null;
		String json = r.getSourceAsString();
		X x = config.getGson().fromJson(json, class1);
		return x;
	}

	public <X> X get(ESPath<X> path, Class<X> class1) {
		return get(path.index(), path.type, path.id, class1);
	}

	
	/**
	 * Convenience for `new SearchRequestBuilder(this)`.
	 * Provided for drop-in similarity to the ES classes.
	 * @param index
	 * @return
	 */
	public SearchRequest prepareSearch(String index) {
		return new SearchRequest(this).setIndex(index);
	}

	public BulkRequest prepareBulk() {
		return new BulkRequest(this);
	}

	public SearchScrollRequest prepareSearchScroll(String scrollId) {
		return new SearchScrollRequest(this, scrollId, TUnit.MINUTE.dt);
	}
	
	public ClearScrollRequest prepareClearScroll() {
		return new ClearScrollRequest(this);
	}

	public void close() {
		if (closed) return;
//		threads.shutdown(); the threads are a shared static pool
		closed = true;
	}

	public UpdateRequest prepareUpdate(ESPath path) {
		UpdateRequest urb = new UpdateRequest(this);
		urb.setPath(path);
		return urb;
	}

	public IndexRequest prepareIndex(ESPath path) {
		IndexRequest urb = new IndexRequest(this);
		urb.setPath(path);
		return urb;
	}

	/**
	 * @deprecated does nothing yet
	 */
	@Override
	public void flush() {
		// what can we do to make sure all the CallES have been submitted??
	}
	
	public TransformRequest prepareTransformPreview() {
		return new TransformRequest(this);
	}

	public TransformRequest prepareTransform(String transform_job) {
		return new TransformRequest(this, transform_job, "PUT");
	}
	
	public TransformRequest prepareTransformStart(String transform_job) {
		return new TransformRequest(this, transform_job+"/_start", "POST");
	}
	
	public TransformRequest prepareTransformStop(String transform_job) {
		return new TransformRequest(this, transform_job+"/_stop", "POST");
	}
	
	public TransformRequest prepareTransformDelete(String transform_job) {
		return new TransformRequest(this, transform_job, "DELETE");
	}
	
}
