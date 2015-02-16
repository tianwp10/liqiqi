package liqiqi.status;

/**
 * many system module contains status while running, and some system else want
 * to know that status in order to moniter or other usage. the module should
 * implements this interface and fill the getStatus method. this helps a lot
 * when in big system development.
 * 
 * @author steventian
 *
 */
public interface StatusCollected {
	public String getStatus();
}
