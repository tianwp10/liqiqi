package liqiqi.aggr;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AggrMap<K, V1, V2> {

	final private ConcurrentHashMap<K, V2> map = new ConcurrentHashMap<K, V2>();
	final private Updater<K, V1, V2> p;

	public AggrMap(Updater<K, V1, V2> p) {
		this.p = p;
	}

	public void add(K k, V1 v) {
		synchronized (map) {
			if (p.inplace() && map.containsKey(k)) {
				p.update(k, v, map.get(k));
			} else {
				map.put(k, p.update(k, v, map.get(k)));
			}
		}
	}

	public V2 get(K k) {
		synchronized (map) {
			return map.get(k);
		}
	}

	public V2 getAndSet(K k, V1 v1) {
		synchronized (map) {
			V2 v2 = map.remove(k);
			add(k, v1);
			return v2;
		}
	}

	public V2 remove(K k) {
		synchronized (map) {
			return map.remove(k);
		}
	}

	public Set<K> keySet() {
		synchronized (map) {
			return map.keySet();
		}
	}

	public Set<Map.Entry<K, V2>> entrySet() {
		synchronized (map) {
			return map.entrySet();
		}
	}

	public int size() {
		return map.size();
	}

	public interface Updater<K, V1, V2> {
		public V2 update(K k, V1 v, V2 finalv);

		public boolean inplace();
	}

}
