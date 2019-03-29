package com.alibaba.jstorm.kafka;

import java.nio.ByteBuffer;

public class ByteBufferAndOffset {
	
	private ByteBuffer byteBuffer;
	
	private long offset;
	
	public ByteBufferAndOffset(ByteBuffer byteBuffer, long offset) {
		super();
		this.byteBuffer = byteBuffer;
		this.offset = offset;
	}

	public ByteBuffer byteBuffer() {
		return byteBuffer;
	}

	public long offset() {
		return offset;
	}

}
