package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.util.ElasticSearchUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

@RestController
public class DemoController {

    @Autowired
    private ElasticSearchUtil elasticSearchUtil;

    @PostMapping("/sink_sync")
    public void demo(HttpServletRequest request) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(request.getInputStream()));
        String line = null;
        StringBuilder sb = new StringBuilder();
        while((line = br.readLine())!=null){
            sb.append(line);
        }

        JSONObject jsonObject = JSONObject.parseObject(sb.toString());
        String operation = jsonObject.getString("operation");
        JSONObject data = jsonObject.getJSONObject("data");

        Map map = (Map) JSON.parse(data.toJSONString());

        switch (operation) {
            case "sink_add_table":
                elasticSearchUtil.insert(map, "plugin-test");
            case "sink_remove_table":
                System.out.println(data.toJSONString());
            case "sink_emit_row_changed_events":
                elasticSearchUtil.insert(map, "plugin-test");
            case "sink_emit_ddl_event":
                System.out.println(data.toJSONString());
        }
    }
}
