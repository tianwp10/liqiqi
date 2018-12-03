package liqiqi.stream;

import java.io.Closeable;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import liqiqi.status.StatusCollected;

/**
 * 
 * @author tianwanpeng
 *
 */
public class TimeoutNoticer<T> implements StatusCollected, Closeable {

	@Override
	public String getStatus() {
		StringBuffer sb = new StringBuffer();
		sb.append("[key2times.size:" + key2times.size() + "]");
		return sb.toString();
	}

	private static Timer timer = null;
	final static private Object timerlock = new Object();

	final private int timeout_ms;
	final private Noticer<T> noticer;
	final private ConcurrentHashMap<T, Long> key2times;
	final private Object lock = new Object();

	public TimeoutNoticer(int timeout_ms, Noticer<T> noticer) {
		this.timeout_ms = timeout_ms;
		this.noticer = noticer;
		this.key2times = new ConcurrentHashMap<T, Long>();

		synchronized (timerlock) {
			if (timer == null) {
				timer = new Timer();
			}
		}
	}

	public void update(T key) {
		this.update(key, System.currentTimeMillis());
	}

	public void insertWtihoutUpdate(T key) {
		this.key2times.putIfAbsent(key, System.currentTimeMillis());
	}

	public void update(T key, long tupleTime) {
		if (!this.key2times.containsKey(key)) {
			synchronized (lock) {
				if (!this.key2times.containsKey(key)) {
					schedule(key);
				}
			}
		}
		this.key2times.put(key, tupleTime);
	}

	public boolean containsKey(String key) {
		return key2times.containsKey(key);
	}

	private void schedule(T key) {
		TimerTask ttask = new TupleTimerTask(key);
		long timeout = timeout_ms;
		if (key2times.containsKey(key)) {
			timeout = Math.min(timeout_ms, key2times.get(key) + timeout_ms
					- System.currentTimeMillis());
			timeout = Math.max(timeout, 0);
		}

		while (true) {
			try {
				timer.schedule(ttask, timeout);
				break;
			} catch (final Throwable e) {
				e.printStackTrace();
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

	private void emit(T key) {
		key2times.remove(key);
		noticer.notice(key);
	}

	@Override
	public void close() throws IOException {
		for (T key : key2times.keySet()) {
			emit(key);
		}
		timer.cancel();
	}

	private class TupleTimerTask extends TimerTask {
		final private T key;

		public TupleTimerTask(T key) {
			this.key = key;
		}

		@Override
		public void run() {
			try {
				long ctime = System.currentTimeMillis();
				if (key2times.containsKey(key)) {
					synchronized (lock) {
						if (key2times.containsKey(key)) {
							if (ctime - key2times.get(key) > timeout_ms) {
								emit(key);
							} else {
								schedule(key);
							}
						}
					}
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	public interface Noticer<T> {
		public void notice(T key);
	}

	public static void main(String[] args) throws IOException {
		TimeoutNoticer<String> tc = new TimeoutNoticer<String>(2000,
				new Noticer<String>() {

					@Override
					public void notice(String key) {
						System.out.println(System.currentTimeMillis() + " "
								+ key);
					}

				});
		long x = 0;
		while (true) {
			tc.update(String.valueOf(x++));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				tc.close();
				break;
			}
		}
	}

}
