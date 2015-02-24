package liqiqi.stream.old;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import liqiqi.status.StatusCollected;

/**
 * 
 * @author tianwanpeng
 *
 */
public class TimeoutCheckerRich<T> implements StatusCollected, Closeable {

	@Override
	public String getStatus() {
		StringBuffer sb = new StringBuffer();
		sb.append("[key2times.size:" + key2times.size() + "]");
		return sb.toString();
	}

	private static Timer timer = null;
	final static private Object timerlock = new Object();

	final private int timeunit_seconds;
	final private int timeout_ms;
	final private boolean considerSystemTime;
	final private CheckerProcessorRich<T> checker;
	final private ConcurrentHashMap<TimeTuple, Long> key2times;
	final private Object lock = new Object();

	public TimeoutCheckerRich(int time_num, TimeUnit unit, int timeout_ms,
			CheckerProcessorRich<T> checker) {
		this(time_num, unit, timeout_ms, true, checker);
	}

	public TimeoutCheckerRich(int time_num, TimeUnit unit, int timeout_ms,
			boolean considerSystemTime, CheckerProcessorRich<T> checker) {
		this.timeunit_seconds = (int) unit.toSeconds(time_num);
		this.timeout_ms = timeout_ms;
		this.considerSystemTime = considerSystemTime;
		this.checker = checker;
		this.key2times = new ConcurrentHashMap<TimeTuple, Long>();

		synchronized (timerlock) {
			if (timer == null) {
				timer = new Timer();
			}
		}
	}

	public void update(T t) {
		this.update(t, System.currentTimeMillis());
	}

	/**
	 * 
	 * @param t
	 * @param tupleTime
	 */
	public void update(T t, long tupleTime) {
		TimeTuple ttkey = new TimeTuple(t, tupleTime, this.timeunit_seconds);
		synchronized (lock) {
			if (!this.key2times.containsKey(ttkey)) {
				schedule(ttkey);
			}
			this.key2times.put(ttkey, System.currentTimeMillis());
		}
	}

	public boolean containsKey(T t) {
		return this.containsKey(t, System.currentTimeMillis());
	}

	public boolean containsKey(T t, long tupleTime) {
		TimeTuple ttkey = new TimeTuple(t, tupleTime, this.timeunit_seconds);
		synchronized (lock) {
			return key2times.containsKey(ttkey);
		}
	}

	private void schedule(TimeTuple ttkey) {
		TimerTask ttask = new TupleTimerTask(ttkey);

		long nextTime = (key2times.containsKey(ttkey) ? key2times.get(ttkey)
				: ttkey.tupleTime) + timeout_ms;
		if (considerSystemTime) {
			long nextSysTime = (int) (ttkey.tupleTime + timeunit_seconds * 1000);
			nextTime = Math.max(nextSysTime, nextTime);
		}
		long ctime = System.currentTimeMillis();
		int timeout = (int) (nextTime - ctime);
		timeout = timeout < 0 ? 0 : timeout;
		while (true) {
			try {
				timer.schedule(ttask, timeout);
				break;
			} catch (final Throwable e) {
				if (e instanceof IllegalStateException) {
					synchronized (timerlock) {
						timer = new Timer();
					}
				} else {
					break;
				}
			}
		}
	}

	private void emit(TimeTuple ttkey) {
		key2times.remove(ttkey);
		checker.process(ttkey.tupleKey, ttkey.tupleTime);
	}

	@Override
	public void close() throws IOException {
		for (TimeTuple ttKey : key2times.keySet()) {
			emit(ttKey);
		}
		timer.cancel();
	}

	private class TimeTuple {
		final T tupleKey;
		final long tupleTime;

		public TimeTuple(T tupleKey, long tupleTime, int timeunit_seconds) {
			this.tupleKey = tupleKey;
			this.tupleTime = tupleTime / timeunit_seconds / 1000
					* timeunit_seconds * 1000;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null || !(obj instanceof TimeoutCheckerRich.TimeTuple)) {
				return false;
			}
			return this.hashCode() == obj.hashCode();
		}

		@Override
		public int hashCode() {
			return (int) (tupleTime * 31 + (tupleKey == null ? 0 : tupleKey
					.hashCode()));
		}
	}

	private class TupleTimerTask extends TimerTask {
		final private TimeTuple ttkey;

		public TupleTimerTask(TimeTuple ttkey) {
			this.ttkey = ttkey;
		}

		@Override
		public void run() {
			try {
				synchronized (lock) {
					long ctime = System.currentTimeMillis();
					if (key2times.containsKey(ttkey)) {
						if (ctime - key2times.get(ttkey) > timeout_ms) {
							emit(ttkey);
						} else {
							schedule(ttkey);
						}
					}
				}
			} catch (Throwable e) {
			}
		}
	}

	public static interface CheckerProcessorRich<T> {
		public String getKey(T t);

		public void process(T key, long tupleTime);
	}

	public static void main(String[] args) throws IOException {
		TimeoutCheckerRich<String> tc = new TimeoutCheckerRich<String>(10,
				TimeUnit.SECONDS, 1000, new CheckerProcessorRich<String>() {

					@Override
					public void process(String key, long tupleTime) {
						SimpleDateFormat format = new SimpleDateFormat(
								"yyyyMMddHHmmss");
						System.out.println(System.currentTimeMillis() + " "
								+ key + " " + format.format(tupleTime));
					}

					@Override
					public String getKey(String t) {
						return t;
					}

				});
		while (true) {
			tc.update(getRandString(), System.currentTimeMillis());
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				tc.close();
				break;
			}
		}
	}

	static Random r = new Random();

	private static String getRandString() {
		int num = 1;
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < num; i++) {
			sb.append((char) ('a' + r.nextInt(2)));
		}

		return sb.toString();
	}
}
