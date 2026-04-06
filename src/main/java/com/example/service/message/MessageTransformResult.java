package com.example.service.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageTransformResult {
    /**
     * DDZ请求结构（与XMLCONFIG.json同型）
     */
    private Map<String, Object> dtoMap;

    /**
     * XML字符串
     */
    private String xml;

    /**
     * CIPS风格签名原文串
     */
    private String signPlainText;
}
