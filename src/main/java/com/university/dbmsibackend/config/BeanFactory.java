package com.university.dbmsibackend.config;

import com.university.dbmsibackend.service.IndexNestedLoopJoinService;
import com.university.dbmsibackend.service.JoinExecutor;
import com.university.dbmsibackend.service.MongoService;
import com.university.dbmsibackend.service.api.JoinService;
import com.university.dbmsibackend.util.JoinUtil;
import com.university.dbmsibackend.util.JsonUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeanFactory {
    @Bean
    public JsonUtil jsonUtil() {
        return new JsonUtil();
    }

    @Bean
    public JoinExecutor joinExecutor(JsonUtil jsonUtil, MongoService mongoService, JoinUtil joinUtil) {
        JoinService indexNestedLoopJoinService = new IndexNestedLoopJoinService(jsonUtil, mongoService, joinUtil);
        return new JoinExecutor(indexNestedLoopJoinService);
    }
}
