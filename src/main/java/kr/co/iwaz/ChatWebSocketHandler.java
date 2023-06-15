package kr.co.iwaz;

import kr.co.iwaz.model.ChatRes;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static j2html.TagCreator.*;
import static kr.co.iwaz.Main.gson;

@WebSocket
public class ChatWebSocketHandler {

    static Map<Session, String> userUsernameMap = new ConcurrentHashMap<>();
    static int nextUserNumber = 1; // 다음 유저 이름을 위한 인덱스

    @OnWebSocketConnect
    public void onConnect(Session user) {
        String username = "User" + nextUserNumber++;
        userUsernameMap.put(user, username);
        broadcastMessage("Server", username + " joined the chat");
    }

    @OnWebSocketClose
    public void onClose(Session user, int statusCode, String reason) {
        String username = userUsernameMap.get(user);
        userUsernameMap.remove(user);
        broadcastMessage("Server", username + " left the chat");
    }

    @OnWebSocketMessage
    public void onMessage(Session user, String message) {
        broadcastMessage(userUsernameMap.get(user), message);
    }

    // 메시지를 모든 유저에게 브로드 캐스팅
    public static void broadcastMessage(String sender, String message) {
        // 응답 메시지 생성
        ChatRes res = new ChatRes();
        res.userMessage = createHtmlMessageFromSender(sender, message);
        res.userlist = userUsernameMap.values();

        userUsernameMap.keySet().stream().filter(Session::isOpen).forEach(session -> {
            try {
                // Websocket 으로 전송
                session.getRemote().sendString(String.valueOf(gson.toJson(res)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    // sender-name, message, timestamp 을 표시하는 HTML element 렌더링
    private static String createHtmlMessageFromSender(String sender, String message) {
        return article().with(
                b(sender + " says:"),
                p(message),
                span().withClass("timestamp").withText(new SimpleDateFormat("HH:mm:ss").format(new Date()))
        ).render();
    }

}
