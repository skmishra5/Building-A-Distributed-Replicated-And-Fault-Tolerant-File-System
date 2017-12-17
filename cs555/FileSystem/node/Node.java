package cs555.FileSystem.node;

import cs555.FileSystem.wireformats.Event;

public interface Node {
	public void onEvent(Event e);
}
