package org.ergemp.fv.kproxy.Servlet.Handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.ergemp.fv.kproxy.Servlet.Response.Response200;
import org.ergemp.fv.kproxy.Servlet.Response.Response400;
import org.ergemp.fv.kproxy.Servlet.Response.Response500;
import org.ergemp.fv.kproxy.actor.SendMessage;
import org.ergemp.fv.kproxy.config.GetKafkaProducer;
import org.ergemp.fv.kproxy.model.GenericDataModel;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

public class RestProxy extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response) throws IOException {

        PrintWriter prt = response.getWriter();
        BufferedReader rdr = request.getReader();

        String readLine;
        StringBuffer sb = new StringBuffer();

        while ((readLine = rdr.readLine()) != null) {
            sb.append(readLine);
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonObj = mapper.readTree(sb.toString());

        if (
                jsonObj.get("topic") != null &&
                jsonObj.get("key") != null &&
                jsonObj.get("value") != null
        ) {
            Producer producer = GetKafkaProducer.get("RestProxy01");


            GenericDataModel model = new GenericDataModel(
                    jsonObj.get("topic").textValue(), //topic
                    System.currentTimeMillis(),
                    jsonObj.get("key").textValue(),
                    jsonObj.get("value").textValue() );

            SendMessage.send(producer,
                                jsonObj.get("topic").textValue(),
                                jsonObj.get("key").textValue(),
                                jsonObj.get("value").textValue() );

            System.out.print(model.toString());

            producer.close();

            Response200.set200(response);
        }
        else {
            Response400.set400(response);
        }

    }

}
