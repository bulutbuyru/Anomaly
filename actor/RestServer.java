package org.ergemp.fv.kproxy.actor;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.ergemp.fv.kproxy.Servlet.Handler.RestProxy;

public class RestServer {
    public static org.eclipse.jetty.server.Server createServer(int port) {
        Server server = new Server(port);

        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);

        handler.addServletWithMapping(RestProxy.class, "/rest/proxy");

        return server;
    }
}
