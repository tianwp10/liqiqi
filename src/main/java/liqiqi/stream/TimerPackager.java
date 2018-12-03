package liqiqi.stream;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import liqiqi.status.StatusCollected;
import liqiqi.stream.ExecutorProcessor.Processor;

public class TimerPackager<T, P> implements StatusCollected, Closeable {

	@Override
	public String getStatus() {
		StringBuffer sb = new StringBuffer();
		return sb.toString();
	}

	final private ConcurrentHashMap<String, P> key2P;

	final private ExecutorProcessor<Msg> executorProcessor;
	final private TimeoutNoticer<String> timeoutNoticer;
	final private AtomicBoolean closeCmd;

	private enum Cmd {
		addTuple, emit
	}

	private class Msg {
		final Cmd cmd;
		final String key;
		final T t;

		public Msg(Cmd cmd, String key, T t) {
			this.cmd = cmd;
			this.key = key;
			this.t = t;
		}
	}

	public TimerPackager(int timeout_ms, final Packager<T, P> packager) {
		this(timeout_ms, 1, packager);
	}

	public TimerPackager(int timeout_ms, final int processor_parallel,
			final Packager<T, P> packager) {
		this(timeout_ms, true, processor_parallel, packager);
	}

	public TimerPackager(int timeout_ms, final boolean timeoutFromLastUpdate,
			final int processor_parallel, final Packager<T, P> packager) {
		this.key2P = new ConcurrentHashMap<String, P>();
		this.closeCmd = new AtomicBoolean(false);

		this.timeoutNoticer = new TimeoutNoticer<String>(timeout_ms,
				new TimeoutNoticer.Noticer<String>() {
					@Override
					public void notice(String key) {
						// here if close cmd received, just emit the package
						// from packager, because ep has allready been closed
						//
						// notice that all the pack action should execute in ep
						// except during the closing state
						if (closeCmd.get()) {
							packager.emit(key, key2P.remove(key), false);
						} else {
							executorProcessor.addTuple(new Msg(Cmd.emit, key,
									null));
						}
					}
				});

		this.executorProcessor = new ExecutorProcessor<Msg>(
				new ExecutorProcessor.Processor<Msg>() {
					@Override
					public String getStatus() {
						return null;
					}

					@Override
					public int hash(Msg t) {
						if (t.cmd == Cmd.addTuple) {
							return packager.getKey(t.t).hashCode();
						} else {
							return t.key.hashCode();
						}
					}

					@Override
					public void process(Msg t, int executorid) {
						if (t.cmd == Cmd.addTuple) {
							String key = packager.getKey(t.t);
							if (timeoutFromLastUpdate) {
								timeoutNoticer.update(key);
							} else {
								timeoutNoticer.insertWtihoutUpdate(key);
							}

							if (!key2P.containsKey(key)) {
								key2P.put(key, packager.newPackage(t.t));
							}
							boolean full = packager.pack(key, t.t,
									key2P.get(key));
							if (full) {
								packager.emit(key, key2P.remove(key), full);
							}
						} else if (t.cmd == Cmd.emit) {
							packager.emit(t.key, key2P.remove(t.key), false);
						}
					}

				}, processor_parallel, 10000, "ep4tp");

		this.executorProcessor.start();
	}

	/**
	 * thread safe
	 * 
	 * @param t
	 */

	public void putTuple(T t) {
		this.executorProcessor.addTuple(new Msg(Cmd.addTuple, null, t));
	}

	@Override
	public void close() throws IOException {
		this.closeCmd.set(true);
		executorProcessor.close();
		timeoutNoticer.close();
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

	public static void main(String[] args) throws IOException,
			InterruptedException {
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

		// Thread.sleep(10000);

		tp.close();
	}
}
