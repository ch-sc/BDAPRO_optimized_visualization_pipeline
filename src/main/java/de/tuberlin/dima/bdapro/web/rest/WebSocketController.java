package de.tuberlin.dima.bdapro.web.rest;

import de.tuberlin.dima.bdapro.model.dto.ScatterPlotMessage;
import de.tuberlin.dima.bdapro.model.StreamedData;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;

@Controller
public class WebSocketController {

//    @MessageMapping("/socket")
//    @SendTo("/scatter/data")
//    public StreamedData greeting(ScatterPlotMessage message) throws Exception {
//
////        ToDo: fetch data from Flink stream processing
////        return new StreamedData("Hello, " + HtmlUtils.htmlEscape(message.toString()) + "!");
//    }


}