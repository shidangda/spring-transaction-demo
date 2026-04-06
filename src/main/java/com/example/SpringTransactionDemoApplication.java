package com.example;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * SpringBoot主启动类
 * 用于学习Spring事务管理
 */
@SpringBootApplication
@MapperScan("com.example.mapper")
@EnableTransactionManagement // 显式启用事务管理（SpringBoot默认已启用，这里为了学习目的显式声明）
public class SpringTransactionDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringTransactionDemoApplication.class, args);
        System.out.println("=================================");
        System.out.println("Spring事务学习项目启动成功！");
        System.out.println("访问地址: http://localhost:8080/api");
        System.out.println("=================================");
    }
}