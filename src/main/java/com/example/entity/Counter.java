package com.example.entity;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Counter {
    private Long id;
    private String name; // 计数器名称（唯一）
    private Long currentMax; // 已分配的最大值
    private Integer step; // 批次大小（固定为50）
    private Integer version; // 乐观锁版本
    private LocalDateTime updatedTime;
}
