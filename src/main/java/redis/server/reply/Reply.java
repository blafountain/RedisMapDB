package redis.server.reply;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public interface Reply<T> {
    byte[] CRLF = new byte[]{RedisReplyDecoder.CR, RedisReplyDecoder.LF};

    T data();

    void write(ByteBuf os) throws IOException;
}
