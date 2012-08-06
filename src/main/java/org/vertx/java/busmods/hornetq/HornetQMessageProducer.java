package org.vertx.java.busmods.hornetq;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class HornetQMessageProducer extends BusModBase implements Handler<Message<JsonObject>> {

    public static final String address = "vertx.mod.hornetq";
    private ClientSession session;
    private ClientProducer producer;

    @Override
    public void start() {
        super.start();
        eb.registerHandler(address, this);
        String host = getOptionalStringConfig("host", "localhost");
        int port = getOptionalIntConfig("port", 5445);
        String defaultAddress = getOptionalStringConfig("defaultAddress", null);
        try {
            bootstrapHornetQ(host, port, defaultAddress);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void bootstrapHornetQ(String host, int port, String address) throws Exception {
        Map<String, Object> connectionParams = new HashMap<String, Object>();
        connectionParams.put(TransportConstants.PORT_PROP_NAME, port);
        connectionParams.put(TransportConstants.HOST_PROP_NAME, host);

        TransportConfiguration transportConfiguration = new TransportConfiguration(
                NettyConnectorFactory.class.getCanonicalName(),
                connectionParams);
        ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(transportConfiguration);
        serverLocator.setBlockOnDurableSend(false);
        ClientSessionFactory factory = serverLocator.createSessionFactory();
        session = factory.createSession(true, true, 0);
        producer = session.createProducer(address);
        session.start();
    }

    @Override
    public void handle(Message<JsonObject> event) {
        String address = event.body.getString("address");
        if(address == null) {
            address = producer.getAddress().toString();
        }
        if(address == null) {
            throw new IllegalArgumentException("You must specify an address for this message");
        }

        ClientMessage message = session.createMessage(true);
        message.setDurable(true);
        message.getBodyBuffer().writeString(event.body.getString("message"));
        try {
            container.getLogger().debug("Sending message to " + address);
            producer.send(address, message);
            message.acknowledge();
        } catch (HornetQException e) {
            sendError(event, "Error sending message: " + e.getMessage());
        }
    }
}
