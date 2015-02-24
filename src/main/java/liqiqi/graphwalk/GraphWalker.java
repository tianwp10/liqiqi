package liqiqi.graphwalk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Stack;

public class GraphWalker<T> {

	final private Stack<Node> opStack;
	final private List<Node> toWalk;
	final private LinkedHashMap<Node, T> retMap;
	final private Dispatcher<T> dispatcher;
	final private WalkMode mode;

	public GraphWalker(Dispatcher<T> dispatcher, WalkMode mode) {
		opStack = new Stack<Node>();
		toWalk = new ArrayList<Node>();
		retMap = new LinkedHashMap<Node, T>();
		this.dispatcher = dispatcher;
		this.mode = mode;
	}

	private void dispatch(Node nd, Stack<Node> ndStack) throws Exception {
		ArrayList<T> childNodeOutputs = getChildrenNodeOutputs(nd);
		T retVal = dispatcher.dispatch(nd, ndStack, childNodeOutputs, retMap);
		retMap.put(nd, retVal);
	}

	private ArrayList<T> getChildrenNodeOutputs(Node nd) {
		if (nd.getChildren() != null) {
			ArrayList<T> nodeOutputs = new ArrayList<T>(nd.getChildren().size());
			for (Node child : nd.getChildren()) {
				nodeOutputs.add(retMap.get(child));
			}
			return nodeOutputs;
		}
		return null;
	}

	public HashMap<Node, T> walk(Collection<Node> startNodes) throws Exception {
		// HashMap<Node, T> nodeOutput = new HashMap<GraphWalker.Node, T>();
		toWalk.addAll(startNodes);
		while (toWalk.size() > 0) {
			Node nd = toWalk.remove(0);
			if (mode == WalkMode.CHILD_FIRST) {
				walkChild(nd);
			} else if (mode == WalkMode.ROOT_FIRST) {
				walkRoot(nd);
			} else if (mode == WalkMode.ROOT_FIRST_RECURSIVE) {
				walkRootRecursive(nd);
			} else if (mode == WalkMode.LEAF_FIRST) {
				walkLeaf(nd);
			}
			// if (nodeOutput != null) {
			// nodeOutput.put(nd, retMap.get(nd));
			// }
		}
		return retMap;
	}

	private void walkChild(Node nd) throws Exception {
		if (opStack.empty() || nd != opStack.peek()) {
			opStack.push(nd);
		}

		if ((nd.getChildren() == null)
				|| retMap.keySet().containsAll(nd.getChildren())
				|| !dispatcher.needToDispatchChildren(nd, opStack,
						getChildrenNodeOutputs(nd), retMap)) {
			if (!retMap.keySet().contains(nd)) {
				dispatch(nd, opStack);
			}
			opStack.pop();
			return;
		}

		toWalk.add(0, nd);
		toWalk.removeAll(nd.getChildren());
		toWalk.addAll(0, nd.getChildren());
	}

	private void walkLeaf(Node nd) throws Exception {
		opStack.push(nd);

		if ((nd.getChildren() == null)
				|| retMap.keySet().containsAll(nd.getChildren())
				|| !dispatcher.needToDispatchChildren(nd, opStack,
						getChildrenNodeOutputs(nd), retMap)) {
			if (!retMap.keySet().contains(nd)) {
				dispatch(nd, opStack);
			}
			opStack.pop();
			return;
		}
		toWalk.removeAll(nd.getChildren());
		toWalk.addAll(0, nd.getChildren());
		toWalk.add(nd);
	}

	private void walkRoot(Node nd) throws Exception {
		opStack.push(nd);
		dispatch(nd, opStack);
		if (nd.getChildren() != null
				&& dispatcher.needToDispatchChildren(nd, opStack,
						getChildrenNodeOutputs(nd), retMap)) {
			for (Node n : nd.getChildren()) {
				if (!retMap.keySet().contains(n)) {
					walkRoot(n);
				}
			}
		}
		opStack.pop();
	}

	private void walkRootRecursive(Node nd) throws Exception {
		opStack.push(nd);
		dispatch(nd, opStack);
		if (nd.getChildren() != null
				&& dispatcher.needToDispatchChildren(nd, opStack,
						getChildrenNodeOutputs(nd), retMap)) {
			for (Node n : nd.getChildren()) {
				walkRootRecursive(n);
			}
		}
		opStack.pop();
	}

	public static enum WalkMode {
		ROOT_FIRST, ROOT_FIRST_RECURSIVE, CHILD_FIRST, LEAF_FIRST
	}

	public static interface Dispatcher<T> {
		public T dispatch(Node nd, Stack<Node> stack, ArrayList<T> nodeOutputs,
				HashMap<Node, T> retMap) throws Exception;

		public boolean needToDispatchChildren(Node nd, Stack<Node> stack,
				ArrayList<T> nodeOutputs, HashMap<Node, T> retMap);
	}

	public static interface Node {
		public List<? extends Node> getChildren();

		public String getName();

	}
}
