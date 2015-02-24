package liqiqi.stream;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import liqiqi.status.StatusCollected;

public class ExecutorProcessor<T> implements StatusCollected, Closeable {

	@Override
	public String getStatus() {
		StringBuffer sb = new StringBuffer();
		sb.append("[executorsNum:" + executorsNum + ",e.status:");
		for (int i = 0; i < executorsNum; i++) {
			sb.append("(" + this.executors.get(i).getStatus() + "),");
		}
		sb.append("p.status:(" + processor.getStatus() + ")");
		sb.append("]");
		return sb.toString();
	}

	final private Processor<T> processor;
	final private int executorsNum;
	final private HashMap<Integer, Executor> executors;
	final private AtomicBoolean closeCmd;

	/**
	 * 
	 * @param processor
	 * @param executorsNum
	 * @param tupleQueueSize
	 *            : less or equal 0 means unlimited queue
	 */
	public ExecutorProcessor(Processor<T> processor, int executorsNum,
			int tupleQueueSize, String execName) {
		this.closeCmd = new AtomicBoolean(false);
		this.processor = processor;
		this.executorsNum = executorsNum;
		this.executors = new HashMap<Integer, Executor>(executorsNum * 2);
		for (int i = 0; i < this.executorsNum; i++) {
			this.executors.put(i, new Executor(i, tupleQueueSize, execName));
		}
	}

	public void start() {
		for (Integer id : this.executors.keySet()) {
			this.executors.get(id).start();
		}
	}

	public boolean addTuple(T t) {
		// if close cmd received, no more tuples will be accepted
		if (this.closeCmd.get()) {
			return false;
		}
		int id = Math.abs(this.processor.hash(t) % this.executorsNum);
		return this.executors.get(id).addTuple(t);
	}

	@Override
	public void close() throws IOException {
		this.closeCmd.set(true);
		for (Executor exec : executors.values()) {
			exec.close();
		}
	}

	private class Executor extends Thread implements StatusCollected, Closeable {

		final AtomicBoolean closeCmd;

		@Override
		public String getStatus() {
			return "e" + id + ":" + tuples.size();
		}

		final private LinkedBlockingQueue<T> tuples;
		final int id;

		public Executor(int id, int tupleQueueSize, String execName) {
			super(execName + "-" + id);
			this.closeCmd = new AtomicBoolean(false);
			this.id = id;
			tuples = tupleQueueSize > 0 ? new LinkedBlockingQueue<T>(
					tupleQueueSize) : new LinkedBlockingQueue<T>();
		}

		@Override
		public void run() {
			while (true) {
				try {
					T t = tuples.poll(1, TimeUnit.SECONDS);
					if (t == null) {
						// no more tuples and close cmd received, then just
						// break the loop
						if (closeCmd.get()) {
							break;
						} else {
							continue;
						}
					}
					processor.process(t, this.id);
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
			synchronized (this.closeCmd) {
				this.closeCmd.notifyAll();
			}
		}

		private boolean addTuple(T t) {
			return tuples.offer(t);
		}

		@Override
		public void close() throws IOException {
			// send close cmd to the thread and wait until all the tuples in the
			// queue been processed
			this.closeCmd.set(true);
			try {
				synchronized (this.closeCmd) {
					this.closeCmd.wait();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * the class implements this interface must be thread safe
	 * 
	 * @param <T>
	 */
	public static interface Processor<T> extends StatusCollected {

		public int hash(T t);

		public void process(T t, int executorid);

	}

	public static void main(String[] args) throws IOException {
		ExecutorProcessor<Integer> ep = new ExecutorProcessor<Integer>(
				new Processor<Integer>() {

					@Override
					public String getStatus() {
						return null;
					}

					@Override
					public void process(Integer t, int id) {
						System.out.println(t);
					}

					@Override
					public int hash(Integer t) {
						return t;
					}
				}, 10, 0, "ThreadTest");

		ep.start();
		for (int i = 0; i < 1000; i++) {
			ep.addTuple(i);
			System.out.println(ep.getStatus());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
		ep.close();
	}
}
