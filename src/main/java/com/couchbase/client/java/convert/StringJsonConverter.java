package com.couchbase.client.java.convert;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.java.document.StringDocument;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

/**
 * Converter for {@link java.lang.String}s.
 */
public class StringJsonConverter implements Converter<StringDocument, String> {

	@Override
	public String decode(final ByteBuf buffer) {
		return buffer.toString(CharsetUtil.UTF_8);
	}

	@Override
	public ByteBuf encode(final String content) {
		return Unpooled.copiedBuffer(content, CharsetUtil.UTF_8);
	}

	@Override
	public StringDocument newDocument(final String id, final String content, final long cas, final int expiry, final ResponseStatus status) {
		return StringDocument.create(id, content, cas, expiry, status);
	}
}
