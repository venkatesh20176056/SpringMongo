package com.paytm.daas.springmongo.utils;

import org.json.JSONObject;

public class LogMessageGenerator {

    public static String getLogMessage(String name, Long value) {
        JSONObject jsonObject = new JSONObject();

        jsonObject.put("metric_name", name);
        jsonObject.put("metric_value", value);

        return jsonObject.toString();
    }

    public static String getLogMessage(String name, int value) {
        JSONObject jsonObject = new JSONObject();

        jsonObject.put("metric_name", name);
        jsonObject.put("metric_value", value);

        return jsonObject.toString();
    }
}
