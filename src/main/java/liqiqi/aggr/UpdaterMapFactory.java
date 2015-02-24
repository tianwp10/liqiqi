package liqiqi.aggr;

import java.util.concurrent.atomic.AtomicLong;

import liqiqi.aggr.AggrMap.Updater;

public class UpdaterMapFactory {
	public static AggrMap<String, Integer, AtomicLong> getIntegerUpdater() {
		return new AggrMap<String, Integer, AtomicLong>(
				new Updater<String, Integer, AtomicLong>() {

					@Override
					public AtomicLong update(String key, Integer v,
							AtomicLong finalv) {
						if (finalv == null) {
							return new AtomicLong(v);
						}
						finalv.addAndGet(v);
						return finalv;
					}

					@Override
					public boolean inplace() {
						return true;
					}
				});
	}
}
