package liqiqi.algr;

import java.util.Arrays;

/**
 * this util helps to permute all the combinations of a integer array. e.g. give
 * [2 3], it emit (0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2) sequence and
 * call the callback function pcb for each tuple (x, y)
 * 
 * @author steventian
 *
 */
public class PermutationUtil {

	public static void permutate(int[] nums, PermutateCallback cb) {
		int[] idxs = new int[nums.length];
		Arrays.fill(idxs, 0);
		for (int i = 0; i < nums[0]; i++) {
			idxs[0] = i;
			innerPermutate(nums, idxs, 1, cb);
		}
	}

	private static void innerPermutate(int[] nums, int[] idxs, int numidx,
			PermutateCallback cb) {
		if (numidx >= nums.length) {
			cb.call(idxs);
			return;
		}
		for (int i = 0; i < nums[numidx]; i++) {
			idxs[numidx] = i;
			innerPermutate(nums, idxs, numidx + 1, cb);
		}
	}

	private static void print(int[] idxs) {
		for (int i = 0; i < idxs.length; i++) {
			System.out.print(idxs[i] + ", ");
		}
		System.out.println();
	}

	public static interface PermutateCallback {
		public void call(int[] idxs);
	}

	public static void main(String[] args) {
		int[] nums = new int[4];
		nums[0] = 2;
		nums[1] = 3;
		nums[2] = 40;
		nums[3] = 10;
		PermutationUtil.permutate(nums, new PermutateCallback() {
			@Override
			public void call(int[] idxs) {
				print(idxs);
			}
		});
	}
}
