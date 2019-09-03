package com.alibaba.datax.common.messages;

import org.zeromq.ZMsg;

public class MdClient {

    public static void main(String[] args) {
        boolean verbose = (args.length > 0 && "-v".equals(args[0]));
        MdCliApi clientSession = new MdCliApi("tcp://localhost:5555", verbose);

        int count;
        for (count = 0; count < 100000; count++) {
            ZMsg request = new ZMsg();
            request.addString("Hello world");
            ZMsg reply = clientSession.send("echo", request);
            if (reply != null)
                reply.destroy();
            else
                break; // Interrupt or failure
        }

        System.out.printf("%d requests/replies processed\n", count);
        clientSession.destroy();
    }
}
