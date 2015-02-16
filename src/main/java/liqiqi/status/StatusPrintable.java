package liqiqi.status;

/**
 * this interface is different from StatusCollect interface that when in some
 * DAG tasks situations, the parent task may trigger its children tasks to do
 * some status printing works, if all the tasks implements this interface the
 * trigger action seems to be a easy thing
 * 
 * @author steventian
 *
 */
public interface StatusPrintable {
	public void printStatus(int printId);
}
