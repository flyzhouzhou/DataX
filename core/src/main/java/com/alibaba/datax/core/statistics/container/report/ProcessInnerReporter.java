package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.common.messages.JobStateStructure;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.common.messages.RedisStorage;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ProcessInnerReporter extends AbstractReporter {

    private Map<String, Long> finishedTables;

    private Map<String, Long> nowTables;


    public ProcessInnerReporter(){
        this.finishedTables = new ConcurrentHashMap<String, Long>() ;
        this.nowTables = new ConcurrentHashMap<String, Long>();
    }

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
        // do nothing
        RedisStorage redisStorage = new RedisStorage();
        JobStateStructure jobStateStructure = redisStorage.readJobStatus(jobId);
        String percent = CommunicationTool.getPercentage(communication);
        jobStateStructure.setPercentage(percent);
        String state = String.valueOf(communication.getState().value());
        jobStateStructure.setState(state);
        Map<String, Long> tableState = CommunicationTool.getCurrentTable(communication);
        parseTableState(tableState, communication.getState());
        if(communication.getState() == State.SUCCEEDED){
            flushAllTableState(tableState);
        }
        jobStateStructure.setNowTables(this.nowTables.keySet());
        jobStateStructure.setFinishedTables(this.finishedTables.keySet());
        redisStorage.writeJobStatus(jobId, jobStateStructure);
    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroupId, communication);
    }

    private void parseTableState(Map<String, Long> tables, State state){
        try{
            if (state == State.WAITING){
                return;
            }
            if(tables == null){
                return;
            }
            for (Map.Entry<String, Long> entry : tables.entrySet()) {
                String key = entry.getKey();
                if(nowTables.containsKey(key)){
                    long nowtime = nowTables.get(key);
                    if(nowtime < entry.getValue()){
                        nowTables.put(key, entry.getValue()); // 更新对应表时间
                    }else{
                        if(System.currentTimeMillis() - nowtime > 5000){ // 大于5s没更新，表示表已读完
                            nowTables.remove(key);
                            finishedTables.put(key, nowtime);
                        }
                    }
                }else{
                    if(finishedTables.containsKey(key)){
                        long finishedtime = finishedTables.get(key);
                        if(finishedtime < entry.getValue()){  // 针对数据库断线重连
                            finishedTables.remove(key);
                            nowTables.put(key, entry.getValue());
                        }
                    }else{
                        nowTables.put(key, entry.getValue());
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void flushAllTableState(Map<String, Long> tables){
        try{
            if(tables == null){
                return;
            }
            for (Map.Entry<String, Long> entry : tables.entrySet()) {
                String key = entry.getKey();
                if(nowTables.containsKey(key)){
                    nowTables.remove(key);
                    finishedTables.put(key, entry.getValue());
                }else{
                    finishedTables.put(key, entry.getValue());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}