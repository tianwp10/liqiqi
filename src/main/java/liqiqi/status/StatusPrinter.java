package liqiqi.status;

/**
 * we always need to print or log something periodically, we can use while(true)
 * to do that, obviously it is not necessary to write these code every time, so
 * you can just use this StatusPrinter and just write your own Printer with your
 * own business logic
 * 
 * @author steventian
 *
 */
public class StatusPrinter extends Thread {

	final private Printer printer;
	final private int printInterval_ms;

	public StatusPrinter(Printer printer) {
		this(printer, 60);
	}

	public StatusPrinter(Printer printer, int printInterval_seconds) {
		this(printer, printInterval_seconds, "StatusPrinter-"
				+ System.currentTimeMillis() / 1000);
	}

	public StatusPrinter(Printer printer, int printInterval_seconds,
			String printerName) {
		super(printerName);
		this.printer = printer;
		this.printInterval_ms = printInterval_seconds * 1000;
		this.setDaemon(true);
	}

	@Override
	public void run() {
		long laststatistictime = System.currentTimeMillis();
		while (true) {
			try {
				long ctime = System.currentTimeMillis();
				if (ctime - laststatistictime > printInterval_ms) {
					laststatistictime = ctime;
					printer.printStatus();
				}
				Thread.sleep(1000);
			} catch (Throwable e) {
			}
		}
	}

	public static interface Printer {
		public void printStatus();
	}

}
