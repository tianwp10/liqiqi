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
		/**
		 * 超时的原理是要在当前数据tupletime的粒度时间，以后的超时时间才能进行提醒。所以每次update的时候，
		 * 可以把updateime设置为当前粒度的结束时间
		 * （例如，粒度是5分钟，当前tupletime是13分，那么把updatetime设置为15分
		 * ，这样noticer会在15分以后的超时时间进行提醒。
		 * 
		 * 但是仅仅这样做还是不足够的，通常数据会延迟，也就是说tupletime会比systemtime小一些，极端情况下tupletime+
		 * timeunit_ms比systemtime还要小的时候
		 * ，这样设置会导致noticer立即超时，达不到超时检测的效果，这个时候需要考虑systemtime。
		 * 
		 * 超时的真正含义是，在当前数据的时间粒度结束以后开始进行超时检查，也就是超时提醒需要设置在粒度结束时间以后的超时时间以后，
		 * 
		 * 另一方面，为什么不直接使用systemtime进行时间设置呢，是因为有的时候数据稀疏，例如在1分的时候来了一条记录，
		 * 后面就再也没有新的记录了，那么这个时候就会在4分的时候超时（假设超时时间为3分钟），
		 * 但是这和上面的原则是冲突的，我们要求在5分以后才能够超时
		 */
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
