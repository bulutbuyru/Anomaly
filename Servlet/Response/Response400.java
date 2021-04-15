package org.ergemp.fv.kproxy.Servlet.Response;

import javax.servlet.http.HttpServletResponse;

public class Response400 {
    public static HttpServletResponse set400 (HttpServletResponse gResponse) {
        gResponse.setHeader("Origin", "http://localhost");
        gResponse.setHeader("Access-Control-Request-Method", "*");
        gResponse.setHeader("Access-Control-Allow-Methods", "*");
        gResponse.addHeader("Access-Control-Allow-Origin", "*");  //http://127.0.0.1
        gResponse.addHeader("Access-Control-Allow-Headers", "*");  //application/json
        gResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        gResponse.setContentType("text/json");
        gResponse.setCharacterEncoding("utf-8");

        return gResponse;
    }
}
