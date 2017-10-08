/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sparkhbaseloganalyzer;

import java.io.Serializable;

/**
 *
 * @author rakib
 */
public class EventLog implements Serializable {

    private String rowKey;
    private String data;
    private String method;
    private long time;

    public EventLog(String rowKey, String data, String method) {

        this.rowKey = rowKey;
        this.data = data;
        this.method = method;
    }
    
    public EventLog(){
    
    }
    public EventLog(String s){
    
        
    }

    public void setRowKey(String rowKey) {
         this.time = Long.parseLong(rowKey.substring(0, 10));
        
    }

    public long getRowKey() {
        return time;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getMethod() {
        return method;
    }
}
