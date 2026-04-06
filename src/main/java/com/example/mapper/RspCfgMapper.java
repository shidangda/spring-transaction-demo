package com.example.mapper;

import com.example.entity.RspCfg;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface RspCfgMapper {

    List<RspCfg> selectByWkeCode(@Param("wkeCode") String wkeCode);
}
