package com.example.mapper;

import com.example.entity.Counter;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface CounterMapper {
    Counter selectByName(@Param("name") String name);

    int initIfAbsent(@Param("name") String name, @Param("step") int step);

    int allocateNextRange(@Param("name") String name,
            @Param("expectedVersion") int expectedVersion,
            @Param("step") int step);
}
