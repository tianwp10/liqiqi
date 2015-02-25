package liqiqi.msgpack;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import liqiqi.io.DataInputBuffer;
import liqiqi.io.DataOutputBuffer;

import org.xerial.snappy.Snappy;

/**
 * 
 * @author tianwanpeng
 * 
 */
public class PackedMsg {
	private static final int DEFAULT_CAPACITY = 4096;

	private final static byte[] MAGIC0 = { (byte) 0xe, (byte) 0x0 };

	/**
	 * capacity: 4096, compress: true, version: 1
	 * 
	 * @return
	 */
	public static Builder getBuilder() {
		return getBuilder(true);
	}

	/**
	 * capacity: 4096, version: 1
	 * 
	 * @param compress
	 * @return
	 */
	public static Builder getBuilder(boolean compress) {
		return getBuilder(DEFAULT_CAPACITY, compress);
	}

	/**
	 * capacity: 4096, compress: true
	 * 
	 * @param v
	 * @return
	 */
	public static Builder getBuilder(int v) {
		return getBuilder(true, v);
	}

	/**
	 * capacity: 4096
	 * 
	 * @param compress
	 * @param v
	 * @return
	 */
	public static Builder getBuilder(boolean compress, int v) {
		return getBuilder(DEFAULT_CAPACITY, compress, Version.of(v));
	}

	/**
	 * version: 1
	 * 
	 * @param capacity
	 * @param compress
	 * @return
	 */
	public static Builder getBuilder(int capacity, boolean compress) {
		return getBuilder(capacity, compress, Version.v1);
	}

	/**
	 * 
	 * @param capacity
	 * @param compress
	 * @param v
	 * @return
	 */
	public static Builder getBuilder(int capacity, boolean compress, Version v) {
		return new Builder(capacity, compress, v);
	}

	public static class PackInfo {
		private Version version;
		private long createTime;
		private long buildTime;
		private boolean compress;
		private int datalen = 0;
		private int msgcnt = 0;
		private LinkedHashMap<String, Integer> attr2msgcnt;

		public PackInfo() {
		}

		public PackInfo(Version version, boolean compress) {
			this.version = version;
			this.compress = compress;
			attr2msgcnt = new LinkedHashMap<String, Integer>();
		}

		private void setCreateTime(long createTime) {
			this.createTime = createTime;
		}

	}

	public static class Builder {
		final private int CAPACITY;
		final private PackInfo packInfo;

		private LinkedHashMap<String, DataBuffer> attr2MsgBuffer;

		public Builder(int capacity, boolean compress, Version v) {
			this.CAPACITY = capacity;
			packInfo = new PackInfo(v, compress);
			attr2MsgBuffer = new LinkedHashMap<String, DataBuffer>();
			reset();
		}

		public void setCreateTime(long createTime) {
			this.packInfo.setCreateTime(createTime);
		}

		public boolean addMsg(String attr, byte[] data) {
			return addMsg(attr, ByteBuffer.wrap(data));
		}

		/**
		 * return false means current msg is big enough, no other data should be
		 * added again, but attention: the input data has already been added,
		 * and if you add another data after return false it can also be added
		 * successfully.
		 * 
		 * @param attr
		 * @param data
		 * @param offset
		 * @param len
		 * @return
		 */
		public boolean addMsg(String attr, byte[] data, int offset, int len) {
			return addMsg(attr, ByteBuffer.wrap(data, offset, len));
		}

		public boolean addMsg(String attr, ByteBuffer data) {
			DataBuffer outputBuffer = attr2MsgBuffer.get(attr);
			if (outputBuffer == null) {
				outputBuffer = new DataBuffer();
				attr2MsgBuffer.put(attr, outputBuffer);
				// attrlen + utflen + meglen + compress
				packInfo.datalen += attr.length() + 2 + 4 + 1;
			}
			int len = data.remaining();
			try {
				outputBuffer.write(data.array(), data.position(), len);
				packInfo.datalen += len + 4;
				packInfo.datalen += 4;
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
			packInfo.msgcnt++;
			return checkLen(attr, len);
		}

		public boolean addMsgs(String attr, ByteBuffer data) {
			boolean res = true;
			Iterator<ByteBuffer> it = getIteratorBuffer(data);
			while (it.hasNext()) {
				res = this.addMsg(attr, it.next());
			}
			return res;
		}

		private boolean checkLen(String attr, int len) {
			return datalen < CAPACITY;
		}

		public boolean isfull() {
			checkMode(true);
			if (datalen >= CAPACITY) {
				return true;
			}
			return false;
		}

		public ByteBuffer build() {
			return build(System.currentTimeMillis() + timeoffset);
		}

		public ByteBuffer build(long createtime) {
			checkMode(true);
			try {
				this.createtime = createtime;
				DataOutputBuffer out = new DataOutputBuffer(CAPACITY);

				writeHeader(out);
				out.writeInt(attr2MsgBuffer.size());

				if (compress) {
					for (String attr : attr2MsgBuffer.keySet()) {
						out.writeUTF(attr);
						DataBuffer data = attr2MsgBuffer.get(attr);
						if (version.intValue() >= Version.v2.intValue()) {
							out.writeInt(data.cnt);
						}
						int guessLen = Snappy.maxCompressedLength(data.out
								.getLength());
						byte[] tmpData = new byte[guessLen];
						int len = Snappy.compress(data.out.getData(), 0,
								data.out.getLength(), tmpData, 0);
						out.writeInt(len + 1);
						out.writeBoolean(compress);
						out.write(tmpData, 0, len);
					}
				} else {
					for (String attr : attr2MsgBuffer.keySet()) {
						out.writeUTF(attr);
						DataBuffer data = attr2MsgBuffer.get(attr);
						if (version.intValue() >= Version.v2.intValue()) {
							out.writeInt(data.cnt);
						}
						out.writeInt(data.out.getLength() + 1);
						out.writeBoolean(compress);
						out.write(data.out.getData(), 0, data.out.getLength());
					}
				}
				writeMagic(out);
				out.close();
				return ByteBuffer.wrap(out.getData(), 0, out.getLength());
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

		private void writeHeader(DataOutputBuffer out) throws IOException {
			writeMagic(out);
			if (version.intValue() >= Version.v1.intValue()) {
				// createtime = System.currentTimeMillis() + timeoffset;
				out.writeLong(createtime);
			}
			if (version.intValue() >= Version.v2.intValue()) {
				out.writeInt(this.getMsgCnt());
			}
		}

		private void writeMagic(DataOutputBuffer out) throws IOException {
			if (version == Version.v1) {
				out.write(MAGIC1[0]);
				out.write(MAGIC1[1]);
			} else if (version == Version.v2) {
				out.write(MAGIC2[0]);
				out.write(MAGIC2[1]);
			} else {
				throw new IOException("wrong version : " + version.intValue());
			}
		}

		public byte[] buildArray() {
			return buildArray(System.currentTimeMillis() + timeoffset);
		}

		public byte[] buildArray(long createtime) {
			ByteBuffer buffer = this.build(createtime);
			if (buffer == null) {
				return null;
			}
			byte[] res = new byte[buffer.remaining()];
			System.arraycopy(buffer.array(), buffer.position(), res, 0,
					res.length);
			return res;
		}

		public void reset() {
			this.attr2MsgBuffer.clear();
			this.datalen = getHeaderLen();
			msgcnt = 0;
		}

		private int getHeaderLen() {
			int len = 4;// magic
			if (version.intValue() >= Version.v1.intValue()) {
				len += 8;// create time
			}
			if (version.intValue() >= Version.v2.intValue()) {
				len += 4;// msgcnt
			}

			return len + 4;// attrcnt
		}

		// for both mode
		public int getMsgCnt() {
			return msgcnt;
		}

		public int getMsgCnt(String attr) {
			if (addmode) {
				return this.attr2MsgBuffer.get(attr).cnt;
			} else {
				return this.attr2Rawdata.get(attr).cnt;
			}
		}

	}

	private enum Version {
		vn(-1), v0(0), v1(1), v2(2);

		private static final Map<Integer, Version> intToTypeMap = new HashMap<Integer, Version>();
		static {
			for (Version type : Version.values()) {
				intToTypeMap.put(type.value, type);
			}
		}

		private final int value;

		private Version(int value) {
			this.value = value;
		}

		public int intValue() {
			return value;
		}

		public static Version of(int v) {
			if (!intToTypeMap.containsKey(v)) {
				return vn;
			}
			return intToTypeMap.get(v);
		}

	}

	static class DataBuffer {
		DataOutputBuffer out;
		int cnt;

		public DataBuffer() {
			out = new DataOutputBuffer();
		}

		public void write(byte[] array, int position, int len)
				throws IOException {
			cnt++;
			out.writeInt(len);
			out.write(array, position, len);
		}
	}

	private int attrcnt = -1;

	// private LinkedHashMap<String, ByteBuffer> attr2Rawdata = null;
	static class DataByteBuffer {
		final ByteBuffer buffer;
		final int cnt;

		public DataByteBuffer(int cnt, ByteBuffer buffer) {
			this.cnt = cnt;
			this.buffer = buffer;
		}
	}

	private LinkedHashMap<String, DataByteBuffer> attr2Rawdata = null;

	// not used right now
	// private LinkedHashMap<String, Integer> attr2index = null;
	private long createtime = -1;
	private boolean parsed = false;
	final private DataInputBuffer parsedInput;

	// for parsed
	private PackedMsg(ByteBuffer buffer, Version magic) throws IOException {
		version = magic;
		addmode = false;
		CAPACITY = 0;
		parsedInput = new DataInputBuffer();
		parsedInput.reset(buffer.array(), buffer.position() + 2,
				buffer.remaining());
		if (version.intValue() >= Version.v1.intValue()) {
			createtime = parsedInput.readLong();
		}

		if (version.intValue() >= Version.v2.intValue()) {
			this.msgcnt = parsedInput.readInt();
		}

		attrcnt = parsedInput.readInt();
	}

	private void parse() throws IOException {
		if (parsed) {
			return;
		}
		attr2Rawdata = new LinkedHashMap<String, DataByteBuffer>(
				attrcnt * 10 / 7);
		for (int i = 0; i < attrcnt; i++) {
			String attr = parsedInput.readUTF();
			int cnt = 0;
			if (version.intValue() >= Version.v2.intValue()) {
				cnt = parsedInput.readInt();
			}
			int len = parsedInput.readInt();
			int pos = parsedInput.getPosition();
			attr2Rawdata.put(
					attr,
					new DataByteBuffer(cnt, ByteBuffer.wrap(
							parsedInput.getData(), pos, len)));
			parsedInput.skip(len);
		}
		parsed = true;
	}

	private static Version getMagic(ByteBuffer buffer) {
		byte[] array = buffer.array();
		if (buffer.remaining() < 4) {
			return Version.vn;
		}
		int pos = buffer.position();
		int rem = buffer.remaining();
		if (array[pos] == MAGIC2[0] && array[pos + 1] == MAGIC2[1]
				&& array[pos + rem - 2] == MAGIC2[0]
				&& array[pos + rem - 1] == MAGIC2[1]) {
			return Version.v2;
		}
		if (array[pos] == MAGIC1[0] && array[pos + 1] == MAGIC1[1]
				&& array[pos + rem - 2] == MAGIC1[0]
				&& array[pos + rem - 1] == MAGIC1[1]) {
			return Version.v1;
		}
		if (array[pos] == MAGIC0[0] && array[pos + 1] == MAGIC0[1]
				&& array[pos + rem - 2] == MAGIC0[0]
				&& array[pos + rem - 1] == MAGIC0[1]) {
			return Version.v0;
		}
		return Version.vn;
	}

	public static PackedMsg parseFrom(byte[] data) {
		return parseFrom(ByteBuffer.wrap(data));
	}

	public static PackedMsg parseFrom(ByteBuffer buffer) {
		Version magic = getMagic(buffer);
		if (magic == Version.vn) {
			return null;
		}

		try {
			return new PackedMsg(buffer, magic);
		} catch (IOException e) {
			return null;
		}
	}

	private void makeSureParsed() {
		if (!parsed) {
			try {
				parse();
			} catch (IOException e) {
			}
		}
	}

	public Set<String> getAttrs() {
		checkMode(false);
		makeSureParsed();
		return this.attr2Rawdata.keySet();
	}

	public byte[] getRawData(String attr) {
		checkMode(false);
		makeSureParsed();
		ByteBuffer buffer = getRawDataBuffer(attr);
		byte[] data = new byte[buffer.remaining()];
		System.arraycopy(buffer.array(), buffer.position(), data, 0,
				buffer.remaining());
		return data;
	}

	public ByteBuffer getRawDataBuffer(String attr) {
		checkMode(false);
		makeSureParsed();
		return this.attr2Rawdata.get(attr).buffer;
	}

	public Iterator<byte[]> getIterator(String attr) {
		checkMode(false);
		makeSureParsed();
		return getIterator(this.attr2Rawdata.get(attr).buffer);
	}

	public Iterator<ByteBuffer> getIteratorBuffer(String attr) {
		checkMode(false);
		makeSureParsed();
		return getIteratorBuffer(this.attr2Rawdata.get(attr).buffer);
	}

	public static Iterator<byte[]> getIterator(byte[] rawdata) {
		return getIterator(ByteBuffer.wrap(rawdata));
	}

	public static Iterator<byte[]> getIterator(ByteBuffer rawdata) {
		try {
			final DataInputBuffer input = new DataInputBuffer();
			byte[] array = rawdata.array();
			int pos = rawdata.position();
			int rem = rawdata.remaining() - 1;
			int compress = array[pos];

			if (compress == 1) {
				byte[] uncompressdata = new byte[Snappy.uncompressedLength(
						array, pos + 1, rem)];
				int len = Snappy.uncompress(array, pos + 1, rem,
						uncompressdata, 0);
				input.reset(uncompressdata, len);
			} else {
				input.reset(array, pos + 1, rem);
			}

			return new Iterator<byte[]>() {

				@Override
				public boolean hasNext() {
					try {
						return input.available() > 0;
					} catch (IOException e) {
						e.printStackTrace();
					}
					return false;
				}

				@Override
				public byte[] next() {
					try {
						int len;
						len = input.readInt();
						byte[] res = new byte[len];
						input.read(res);
						return res;
					} catch (IOException e) {
						e.printStackTrace();
					}
					return null;
				}

				@Override
				public void remove() {
					this.next();
				}
			};
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	public static Iterator<ByteBuffer> getIteratorBuffer(byte[] rawdata) {
		return getIteratorBuffer(ByteBuffer.wrap(rawdata));
	}

	public static Iterator<ByteBuffer> getIteratorBuffer(ByteBuffer rawdata) {

		try {
			final DataInputBuffer input = new DataInputBuffer();
			byte[] array = rawdata.array();
			int pos = rawdata.position();
			int rem = rawdata.remaining() - 1;
			int compress = array[pos];

			if (compress == 1) {
				byte[] uncompressdata = new byte[Snappy.uncompressedLength(
						array, pos + 1, rem)];
				int len = Snappy.uncompress(array, pos + 1, rem,
						uncompressdata, 0);
				input.reset(uncompressdata, len);
			} else {
				input.reset(array, pos + 1, rem);
			}

			final byte[] uncompressdata = input.getData();

			return new Iterator<ByteBuffer>() {

				@Override
				public boolean hasNext() {
					try {
						return input.available() > 0;
					} catch (IOException e) {
						e.printStackTrace();
					}
					return false;
				}

				@Override
				public ByteBuffer next() {
					try {
						int len = input.readInt();
						int pos = input.getPosition();
						input.skip(len);
						return ByteBuffer.wrap(uncompressdata, pos, len);
					} catch (IOException e) {
						e.printStackTrace();
					}
					return null;
				}

				@Override
				public void remove() {
					this.next();
				}
			};
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	public long getCreatetime() {
		return createtime;
	}

	public int getAttrCount() {
		checkMode(false);
		return attrcnt;
	}

}
