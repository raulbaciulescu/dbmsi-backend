package com.university.dbmsibackend.config;

import com.university.dbmsibackend.service.IndexNestedLoopJoinService;
import com.university.dbmsibackend.service.JoinExecutor;
import com.university.dbmsibackend.service.MongoService;
import com.university.dbmsibackend.service.SortMergeJoinService;
import com.university.dbmsibackend.service.api.JoinService;
import com.university.dbmsibackend.util.JoinUtil;
import com.university.dbmsibackend.util.JsonUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeanFactory {
    @Bean
    public JsonUtil jsonUtil() {
        return new JsonUtil();
    }

    @Bean
    @ConditionalOnProperty(name = "join.service.type", havingValue = "index")
    public JoinExecutor indexNestedLoopJoinExecutor(JsonUtil jsonUtil, MongoService mongoService, JoinUtil joinUtil) {
        JoinService indexNestedLoopJoinService = new IndexNestedLoopJoinService(jsonUtil, mongoService, joinUtil);
        return new JoinExecutor(indexNestedLoopJoinService);
    }

    @Bean
    @ConditionalOnProperty(name = "join.service.type", havingValue = "sort")
    public JoinExecutor sortMergeJoinExecutor(JsonUtil jsonUtil, MongoService mongoService, JoinUtil joinUtil) {
        JoinService sortMergeJoinService = new SortMergeJoinService(jsonUtil, mongoService, joinUtil);
        return new JoinExecutor(sortMergeJoinService);
    }
}
