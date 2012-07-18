package not.org.vertx.java.busmods.hornetq;

import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.vertx.java.busmods.hornetq.HornetQMessageProducer;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.junit4.support.RunInVertx;
import org.vertx.junit4.support.annotations.Verticle;
import org.vertx.junit4.support.annotations.Verticles;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HornetQMessageProducerTest {

    private static final int VERTX_PORT = 8383;
    private static final String QUEUE_ADDRESS = "testQueue";

    @Rule public TestRule chain = RuleChain
            .outerRule(new ExternalResource() {
                private HornetQServer hornetQServer;
                @Override
                protected void before() throws Throwable {
                    Configuration conf = new ConfigurationImpl();
                    conf.setPersistenceEnabled(false);
                    conf.setSecurityEnabled(false);
                    conf.setClustered(false);
                    conf.setQueueConfigurations(
                            Arrays.asList(new CoreQueueConfiguration(QUEUE_ADDRESS, QUEUE_ADDRESS, null, true))
                    );
                    conf.setAcceptorConfigurations(new HashSet<TransportConfiguration>(
                            Arrays.asList(
                                    new TransportConfiguration(
                                            NettyAcceptorFactory.class.getCanonicalName())
                            )
                    ));
                    hornetQServer = HornetQServers.newHornetQServer(conf);
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                hornetQServer.start();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }).start();
                };
                @Override
                protected void after() {
                    try {
                        hornetQServer.stop();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
            })
            .around(new RunInVertx());

    @Test
    @Verticles({
        @Verticle("org.vertx.java.busmods.hornetq.HornetQMessageProducer"),
        @Verticle("not.org.vertx.java.busmods.hornetq.HornetQMessageProducerTest$SendMessageVerticle")
    })
    public void testStart() throws Exception {
        HttpClient httpclient = new DefaultHttpClient();
        try {
            final HttpGet httpget = new HttpGet("http://localhost:" + VERTX_PORT + "/");
            ResponseHandler<String> responseHandler = new BasicResponseHandler();
            httpclient.execute(httpget, responseHandler);
        } finally {
            httpclient.getConnectionManager().shutdown();
        }

        consumeMessage();
    }

    private void consumeMessage() throws Exception {
        TransportConfiguration transportConfiguration = new TransportConfiguration(
                NettyConnectorFactory.class.getCanonicalName());
        ClientSessionFactory factory = HornetQClient.createServerLocatorWithoutHA(transportConfiguration).createSessionFactory();
        ClientSession session = factory.createSession();
        session.start();
        ClientConsumer consumer = session.createConsumer(QUEUE_ADDRESS);
        ClientMessage message = consumer.receiveImmediate();
        assertNotNull(message);
        assertEquals("value", new JsonObject(message.getBodyBuffer().readString()).getString("key"));
    }

    public static class SendMessageVerticle extends org.vertx.java.deploy.Verticle {

        @Override
        public void start() throws Exception {
            vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {

                @Override
                public void handle(HttpServerRequest req) {
                    vertx.eventBus().send(HornetQMessageProducer.address,
                            new JsonObject(new HashMap<String, Object>() {{
                                put("address", QUEUE_ADDRESS);
                                put("message", new JsonObject(new HashMap<String, Object>(){{
                                                    put("key", "value");
                                                }})
                                                .encode()
                                );
                            }})
                    );
                    req.response.end();
                }
            }).listen(VERTX_PORT);
        }
    }
}
