package com.winterwell.es.client;

import java.util.Map;
import java.util.Objects;

import com.winterwell.gson.Gson;
import com.winterwell.utils.containers.ArrayMap;
import com.winterwell.utils.web.IHasJson;
import com.winterwell.web.ajax.JThing;

/**
 * Wrap an individual ES search result
 * @author daniel
 *
 * @param <T>
 */
public final class ESHit<T> implements IHasJson {
	
	@Override
	public String toString() {
		return "ESHit[_index=" + _index + ", id=" + getId() + ", type=" + type + "]";
	}
	@Override
	public int hashCode() {
		return Objects.hash(getId(), _index);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ESHit other = (ESHit) obj;
		return Objects.equals(getId(), other.getId()) && Objects.equals(_index, other._index);
	}
	String _index;
	String _id;
	Map _source;
	private JThing<T> jthing;
	private Class<? extends T> type;
	
	public ESHit() {
		
	}
	public ESHit(JThing<T> jthing) {
		this.jthing = jthing;
	}

	public String getIndex() {
		return _index;
	}

	public String getId() {
		if (_id==null) {
			// hack - get from the map in a standardish field
			_id = (String) getJThing().map().get("id");
		}
		return _id;
	}
	
	/**
	 * JThing wrapper around the source object
	 * @return never null
	 */
	public JThing<T> getJThing() {
		if (jthing==null) {
			assert _source != null;
			jthing = new JThing().setMap(_source).setType(type);
		}
		return jthing;
	}
	ESHit<T> setType(Class<? extends T> type) {
		this.type = type;
		return this;
	}
	
	/**
	 * This recreates the ES json format
	 */
	@Override
	public Map<String,Object> toJson2() throws UnsupportedOperationException {		
		return new ArrayMap(
			"_id", _id,
			"_index", _index,
			"_source", getJThing().map() // make sure we have a map, since _source might be blank
		);
	}
}
