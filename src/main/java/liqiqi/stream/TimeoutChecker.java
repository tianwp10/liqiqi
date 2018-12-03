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
 *         TimeoutChecker的功能，对输入数据按照key+时间粒度进行超时检查，超时以后调用check函数。一个实际的使用场景是数据分拣
 *         ，一个topic会包含很多tid，每个tid按照小时进行数 据汇集，同时，在一定的超时时间（3分钟）内，调用process函数进行处理。
 * 
 *         用户调用函数，需要传入参数，key， tupletime，在具体执行的时候，使用tupletime进行数据粒度计算（小时，5分钟等），
 *         通过considerSystemTime来決定使用系統时间或者tupletime时间进行超时判断
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
