package liqiqi.stream;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import liqiqi.status.StatusCollected;
import liqiqi.stream.TimeoutNoticer.Noticer;

/**
 * 
 * @author tianwanpeng
 *
 */
public class TimeoutChecker implements StatusCollected, Closeable {

	@Override
	public String getStatus() {
		return this.timeoutNoticer.getStatus();
	}

	final private long timeunit_ms;
	final private boolean considerSystemTime;

	final private TimeoutNoticer<String> timeoutNoticer;

	public TimeoutChecker(int time_num, TimeUnit unit, int timeout_ms,
			CheckerProcessor checker) {
		this(time_num, unit, timeout_ms, true, checker);
	}

	public TimeoutChecker(int time_num, TimeUnit unit, int timeout_ms,
			boolean considerSystemTime, final CheckerProcessor checker) {
		this.timeunit_ms = unit.toMillis(time_num);
		this.considerSystemTime = considerSystemTime;

		this.timeoutNoticer = new TimeoutNoticer<String>(timeout_ms,
				new Noticer<String>() {

					@Override
					public void notice(String key) {
						int split = key.lastIndexOf("-");
						checker.process(key.substring(0, split),
								Long.valueOf(key.substring(split + 1)));
					}
				});
	}

	/**
	 * 
	 * @param t
	 * @param tupleTime
	 */
	public void update(String key, long tupleTime) {
		long tupleUnitTime = tupleTime - tupleTime % timeunit_ms;
		this.timeoutNoticer.update(
				key + "-" + tupleUnitTime,
				considerSystemTime ? Math.max(System.currentTimeMillis(),
						tupleUnitTime + timeunit_ms) : tupleTime);
	}

	public boolean containsKey(String key, long tupleTime) {
		long tupleUnitTime = tupleTime - tupleTime % timeunit_ms;
		return this.timeoutNoticer.containsKey(key + "-" + tupleUnitTime);
	}

	@Override
	public void close() throws IOException {
		this.timeoutNoticer.close();
	}

	public static interface CheckerProcessor {
		public void process(String key, long tupleTime);
	}

	public static void main(String[] args) throws IOException {
		TimeoutChecker tc = new TimeoutChecker(10, TimeUnit.SECONDS, 1000,
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
