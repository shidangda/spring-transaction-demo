package com.example.controller;

import com.example.service.message.MessageTransformResult;
import com.example.service.message.XmlDdzTransformService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * 报文转换端到端测试接口
 */
@RestController
@RequestMapping("/message/e2e")
@RequiredArgsConstructor
@Slf4j
public class MessageE2eTestController {

    private final XmlDdzTransformService xmlDdzTransformService;

    /**
     * XML -> DDZ（并返回签名串）
     * POST /api/message/e2e/xml-to-ddz
     */
    @PostMapping("/xml-to-ddz")
    public ResponseEntity<Map<String, Object>> xmlToDdz(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String wkeCode = stringVal(request.get("wkeCode"));
            String xml = stringVal(request.get("xml"));
            validate(wkeCode, "wkeCode不能为空");
            validate(xml, "xml不能为空");

            MessageTransformResult result = xmlDdzTransformService.xmlToDdz(wkeCode, xml);
            response.put("success", true);
            response.put("message", "XML转DDZ成功");
            response.put("data", result);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("XML转DDZ失败", e);
            response.put("success", false);
            response.put("message", "XML转DDZ失败: " + e.getMessage());
            response.put("data", null);
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * DDZ -> XML（并返回签名串）
     * POST /api/message/e2e/ddz-to-xml
     */
    @PostMapping("/ddz-to-xml")
    public ResponseEntity<Map<String, Object>> ddzToXml(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String wkeCode = stringVal(request.get("wkeCode"));
            validate(wkeCode, "wkeCode不能为空");

            Object rawDtoMap = request.get("dtoMap");
            if (!(rawDtoMap instanceof Map)) {
                throw new IllegalArgumentException("dtoMap必须是对象");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> dtoMap = (Map<String, Object>) rawDtoMap;

            MessageTransformResult result = xmlDdzTransformService.ddzToXml(wkeCode, dtoMap);
            response.put("success", true);
            response.put("message", "DDZ转XML成功");
            response.put("data", result);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("DDZ转XML失败", e);
            response.put("success", false);
            response.put("message", "DDZ转XML失败: " + e.getMessage());
            response.put("data", null);
            return ResponseEntity.badRequest().body(response);
        }
    }

    private String stringVal(Object val) {
        return val == null ? null : String.valueOf(val).trim();
    }

    private void validate(String val, String msg) {
        if (val == null || val.isEmpty()) {
            throw new IllegalArgumentException(msg);
        }
    }
}
