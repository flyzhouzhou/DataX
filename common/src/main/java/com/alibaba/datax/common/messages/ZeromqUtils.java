package com.alibaba.datax.common.messages;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class ZeromqUtils {

//    private ZMQ.Context context;
//
//    private ZMQ.Socket socket;
//
//    public ZeromqUtils(){
//        context = ZMQ.context(1);
//        socket = context.socket(ZMQ.REQ);
//        socket.connect("tcp://localhost:5555");
//    }
//
//    public void finalize(){
//        try{
//            socket.close();
//            context.term();
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//    }
//    public void produceMessage(String msg){
//        socket.send(msg.getBytes());
//        socket.recv(0);
//    }


//    public static void main(String args[]){
//        ZMQ.Context context = ZMQ.context(1);  //这个表示创建用于一个I/O线程的context
//
//        ZMQ.Socket socket = context.socket(ZMQ.REP);  //创建一个response类型的socket，他可以接收request发送过来的请求，其实可以将其简单的理解为服务端
//        socket.bind ("tcp://*:5555");    //绑定端口
//        while (true) {
//            byte[] request = socket.recv();  //获取request发送过来的数据
//            System.out.println("receive : " + new String(request));
//            String response = "world";
//            socket.send(response.getBytes());  //向request端发送数据  ，必须要要request端返回数据，没有返回就又recv，将会出错，这里可以理解为强制要求走完整个request/response流程
//        }
//    }


    private MdCliApi2 clientSession;

    public ZeromqUtils(){
        clientSession = new MdCliApi2("tcp://localhost:5555", false);
    }

    public void finalize(){
        clientSession.destroy();
    }

    public void produceMessage(String msg){
        ZMsg request = new ZMsg();
        request.addString(msg);
        clientSession.send("echo", request);
    }

    public static void main(String args[]){
        boolean verbose = (args.length > 0 && "-v".equals(args[0]));
        MdWrkApi workerSession = new MdWrkApi("tcp://localhost:5555", "echo", verbose);

        ZMsg reply = null;
//        ZMsg reply = new ZMsg();
//        reply.addString("reply");
        while (!Thread.currentThread().isInterrupted()) {
            ZMsg request = workerSession.receive(reply);
            if (request == null)
                break; //Interrupted
            reply = request; //  Echo is complex... :-)
        }
        workerSession.destroy();
    }
}
