package kr.co.iwaz;

import static kr.co.iwaz.ChatWebSocketHandler.broadcastMessage;
import static spark.Spark.*;

public class Chat {

    public static void main(String[] args) throws InterruptedException {
        staticFileLocation("/public"); //index.html 는 다음 위치에서 서비스됨 localhost:4567 (default port)
        webSocket("/chat", ChatWebSocketHandler.class);

        init();

        long realTimeDataCounter = 0;
        while (true) {
            broadcastMessage("백그라운드 메시지", "실시간 데이터" + realTimeDataCounter);
            realTimeDataCounter++;
            Thread.sleep(1000);
        }
    }
}