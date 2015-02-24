package liqiqi.stream.old;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import liqiqi.status.StatusCollected;

/**
 * 
 * @author tianwanpeng
 *
 */
public class TimeoutCheckerWithRich implements StatusCollected, Closeable {

	@Override
	public String getStatus() {
		return this.timeoutCheker.getStatus();
	}

	final private TimeoutCheckerRich<String> timeoutCheker;

	public TimeoutCheckerWithRich(int time_num, TimeUnit unit, int timeout_ms,
			CheckerProcessor checker) {
		this(time_num, unit, timeout_ms, true, checker);
	}

	public TimeoutCheckerWithRich(int time_num, TimeUnit unit, int timeout_ms,
			boolean considerSystemTime, final CheckerProcessor checker) {
		this.timeoutCheker = new TimeoutCheckerRich<String>(time_num, unit,
				timeout_ms, considerSystemTime,
				new TimeoutCheckerRich.CheckerProcessorRich<String>() {

					@Override
					public String getKey(String t) {
						return t;
					}

					@Override
					public void process(String key, long tupleTime) {
						checker.process(key, tupleTime);
					}
				});
	}

	public void update(String key) {
		this.update(key, System.currentTimeMillis());
	}

	/**
	 * 
	 * @param t
	 * @param tupleTime
	 */
	public void update(String key, long tupleTime) {
		this.timeoutCheker.update(key, tupleTime);
	}

	public boolean containsKey(String key) {
		return this.containsKey(key, System.currentTimeMillis());
	}

	public boolean containsKey(String t, long tupleTime) {
		return this.timeoutCheker.containsKey(t, tupleTime);
	}

	@Override
	public void close() throws IOException {
		this.timeoutCheker.close();
	}

	public static interface CheckerProcessor {
		public void process(String key, long tupleTime);
	}

	public static void main(String[] args) throws IOException {
		TimeoutCheckerWithRich tc = new TimeoutCheckerWithRich(10, TimeUnit.SECONDS, 1000,
				new CheckerProcessor() {

					@Override
					public void process(String key, long tupleTime) {
						SimpleDateFormat format = new SimpleDateFormat(
								"yyyy-MM-dd HH:mm:ss");
						System.out.println(System.currentTimeMillis() + " "
								+ key + " " + format.format(tupleTime));
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
