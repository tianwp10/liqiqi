package liqiqi.stream.old;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import liqiqi.status.StatusCollected;

public class TimerPackager<T, P> extends Thread implements StatusCollected,
		Closeable {

	@Override
	public String getStatus() {
		StringBuffer sb = new StringBuffer();
		sb.append("[key2times.size:" + key2times.size());
		sb.append(",tobeEmittedKeys.size:" + tobeEmittedKeys.size() + "]");
		return sb.toString();
	}

	private static Timer timer = null;
	final static private Object timerlock = new Object();

	final private int timeout_ms;
	final private Packager<T, P> packager;
	final private ConcurrentHashMap<String, P> key2packages;
	final private ConcurrentHashMap<String, Long> key2times;
	final private ConcurrentHashMap<String, TimerTask> key2timertasks;
	final private LinkedBlockingQueue<EmitPack> tobeEmittedKeys;
	final private Object lock = new Object();

	public TimerPackager(int timeout_ms, Packager<T, P> packager) {
		this.timeout_ms = timeout_ms;
		this.packager = packager;
		key2packages = new ConcurrentHashMap<String, P>();
		key2times = new ConcurrentHashMap<String, Long>();
		key2timertasks = new ConcurrentHashMap<String, TimerTask>();
		tobeEmittedKeys = new LinkedBlockingQueue<EmitPack>(10000);
		synchronized (timerlock) {
			if (timer == null) {
				timer = new Timer();
			}
		}
		this.start();
	}

	/**
	 * thread safe
	 * 
	 * @param t
	 */
	public void putTuple(T t) {
		synchronized (lock) {
			final String key = this.packager.getKey(t);
			P p;
			if (!this.key2times.containsKey(key)) {
				p = this.packager.newPackage(t);
				final long ctime = System.currentTimeMillis();
				this.key2times.putIfAbsent(key, ctime);
				P op = this.key2packages.putIfAbsent(key, p);
				if (op != null) {
					p = op;
				}
				TimerTask ttask = new TimerTask() {
					@Override
					public void run() {
						try {
							synchronized (lock) {
								if (key2times.containsKey(key)
										&& ctime == key2times.get(key)) {
									key2times.remove(key);
									key2timertasks.remove(key);
	
									if (!tobeEmittedKeys.add(new EmitPack(key,
											key2packages.remove(key), false))) {
									}
								}
							}
						} catch (Throwable e) {
						}
					}
				};
	
				TimerTask oldttask = this.key2timertasks.remove(key);
				if (oldttask != null) {
					oldttask.cancel();
				}
				this.key2timertasks.putIfAbsent(key, ttask);
	
				while (true) {
					try {
						timer.schedule(ttask, timeout_ms);
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
			} else {
				p = this.key2packages.get(key);
			}
			if (this.packager.pack(key, t, p)) {
				if (!tobeEmittedKeys.add(new EmitPack(key, key2packages
						.remove(key), true))) {
				}
			}
		}
	}

	private class EmitPack {
		final String key;
		final P p;
		final boolean full;

		public EmitPack(String key, P p, boolean full) {
			this.key = key;
			this.p = p;
			this.full = full;
		}
	}

	@Override
	public void run() {
		while (true) {
			try {
				EmitPack key = tobeEmittedKeys.poll(1, TimeUnit.SECONDS);
				if (key != null) {
					packager.emit(key.key, key.p, key.full);
				}
			} catch (Throwable e) {
			}
		}
	}

	@Override
	public void close() throws IOException {

	}

	public static interface Packager<T, P> {
		public String getKey(T t);

		public P newPackage(T t);

		/**
		 * return true if the package is full
		 * 
		 * @param key
		 * 
		 * @param t
		 * @param p
		 * @return
		 */
		public boolean pack(String key, T t, P p);

		public void emit(String key, P p, boolean full);
	}

	public static void main(String[] args) throws IOException {
		TimerPackager<String, StringBuffer> tp = new TimerPackager<String, StringBuffer>(
				3000, new Packager<String, StringBuffer>() {

					@Override
					public boolean pack(String key, String t, StringBuffer p) {
						p.append(t).append(",");
						return false;
					}

					@Override
					public StringBuffer newPackage(String t) {
						return new StringBuffer();
					}

					@Override
					public String getKey(String t) {
						return t;
					}

					@Override
					public void emit(String key, StringBuffer p, boolean full) {
						System.out.println(p.toString());
					}
				});

		Random r = new Random();
		for (int i = 0; i < 100; i++) {
			tp.putTuple(String.valueOf((char) ('a' + r.nextInt(26))));
		}

		tp.close();
	}
}
