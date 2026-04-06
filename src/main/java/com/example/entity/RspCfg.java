package com.example.entity;

import lombok.Data;

@Data
public class RspCfg {
    private Long id;
    private String wkeCode;
    private String xpath;
    private String ddzName;
    private String fieldName;
    private Integer fieldSeq;
    private String multiTag;
}
