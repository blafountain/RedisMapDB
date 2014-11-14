package redis.server;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.mapdb.DB;
import redis.server.backend.mapdb.MapDBRedisBuilder;
import redis.server.backend.mapdb.MapDBRedisServer;
import redis.server.backend.simple.SimpleRedisServer;

/**
 * Redis server
 */
public class Main {
    @Argument(alias = "p")
    private static Integer port = 6380;

    // TODO: move this stuff out to configuration files
    // mapdb
    @Argument(alias = "l")
    private static String location = "/tmp/redismapdb";

    @Argument(alias = "m")
    private static Boolean dbMemory = false;

    @Argument(alias = "t")
    private static Boolean dbTransactions = false;

    @Argument(alias = "c")
    private static Boolean dbCommit = false;

    //
    @Argument(alias = "backend")
    private static String backend = "mapdb";

    @Argument(alias = "threads")
    private static Integer threads = 1;

    public static void main(String[] args) throws InterruptedException {
        try {
            Args.parse(Main.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(Main.class);
            System.exit(1);
        }

        RedisServer redisServer = null;

        if(backend.equals("mapdb")) {
            System.out.println(" -- " + dbMemory + " -- " + dbCommit + " -- " + dbTransactions);
            DB db = MapDBRedisBuilder.generateDB(location, dbMemory, dbTransactions);

            redisServer = new MapDBRedisServer(db, dbCommit);
        } else {
            // Only execute the command handler in a single thread
            threads = 1;
            redisServer = new SimpleRedisServer();
        }

        final RedisCommandHandler commandHandler = new RedisCommandHandler(redisServer);

        // Configure the server.
        ServerBootstrap b = new ServerBootstrap();
        final DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(threads);
        try {
            b.group(new NioEventLoopGroup(), new NioEventLoopGroup())
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 100)
                    .localAddress(port)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
//             p.addLast(new ByteLoggingHandler(LogLevel.INFO));
                            p.addLast(new RedisCommandDecoder());
                            p.addLast(new RedisReplyEncoder());
                            p.addLast(group, commandHandler);
                        }
                    });

            // Start the server.
            ChannelFuture f = b.bind().sync();

            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        } finally {
            // Shut down all event loops to terminate all threads.
            group.shutdownGracefully();
        }
    }
}
