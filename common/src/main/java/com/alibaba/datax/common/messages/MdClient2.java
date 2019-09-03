package com.alibaba.datax.common.messages;

import org.zeromq.ZMsg;

public class MdClient2 {
    public static void main(String[] args) {
        boolean verbose = (args.length > 0 && "-v".equals(args[0]));
        MdCliApi2 clientSession = new MdCliApi2("tcp://localhost:5555", verbose);

        int count;
        for (count = 0; count < 100000; count++) {
            ZMsg request = new ZMsg();
            request.addString("Hello world");
            clientSession.send("echo", request);
            try{
              //  Thread.currentThread().sleep(100);//毫秒
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        for (count = 0; count < 100000; count++) {
            ZMsg reply = clientSession.recv();
            if (reply != null)
                reply.destroy();
            else
                break; // Interrupt or failure
        }

        System.out.printf("%d requests/replies processed\n", count);
        clientSession.destroy();
    }
}
