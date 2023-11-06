package com.university.dbmsibackend.config;

import com.university.dbmsibackend.util.JsonUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeanFactory {
    @Bean
    public JsonUtil jsonUtil() {
        return new JsonUtil();
    }
}
