package kr.co.iwaz;

import com.google.gson.Gson;
import kr.co.iwaz.model.CommandReq;

import java.util.concurrent.CompletableFuture;

import static kr.co.iwaz.ChatWebSocketHandler.broadcastMessage;
import static spark.Spark.*;

public class Main {

    static Gson gson = new Gson();

    public static void main(String[] args) throws InterruptedException {
        initBackgroundDataSend();
        initRestAPI();
        init();

        long realTimeDataCounter = 0;
        while (true) {
            broadcastMessage("백그라운드 메시지", "실시간 데이터" + realTimeDataCounter);
            realTimeDataCounter++;
            Thread.sleep(1000);
        }
    }

    // 백그라운드에서 실시간으로 websocket 에 데이터를 전송 기능 초기화
    private static void initBackgroundDataSend() throws InterruptedException {
        staticFileLocation("/public"); //index.html 는 다음 위치에서 서비스됨 localhost:4567 (default port)
        webSocket("/chat", ChatWebSocketHandler.class);
    }

    // RestAPI <-> Kafka 연동 기능 초기화
    private static void initRestAPI() {
        WebSocketIdParser parser = data -> {
            CommandReq command = gson.fromJson(data, CommandReq.class);
            return command.websocket_id;
        };

        SyncKafka syncKafka = new SyncKafka("192.168.0.218", 9092, "sync", 5, parser);
        syncKafka.start();

        post("/command", (request, response) -> {
            String body = request.body();
            System.out.println("받은 메시지: " + body);
            CompletableFuture<String> future = syncKafka.send("test", body);
            String kafkaRes = future.get();
            System.out.println(kafkaRes);
            return kafkaRes;
        });
    }
}