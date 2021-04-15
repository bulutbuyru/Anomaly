package org.ergemp.fv.kproxy.runner;

import org.eclipse.jetty.server.Server;
import org.ergemp.fv.kproxy.actor.RestServer;

public class RunRest {

    public static void main(String[] args) throws Exception {
        Server server = RestServer.createServer(8092);
        server.start();
        server.join();
    }
}
