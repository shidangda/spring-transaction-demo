package com.example.service.message;

import com.example.entity.RspCfg;
import com.example.mapper.RspCfgMapper;
import com.example.service.message.transform.DdzToXmlPacker;
import com.example.service.message.transform.SignPlainTextBuilder;
import com.example.service.message.transform.TransformDefinitionResolver;
import com.example.service.message.transform.XmlToDdzUnpacker;
import lombok.RequiredArgsConstructor;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.springframework.stereotype.Service;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class XmlDdzTransformService {

    private final RspCfgMapper rspCfgMapper;
    private final TransformDefinitionResolver definitionResolver;
    private final XmlToDdzUnpacker xmlToDdzUnpacker;
    private final DdzToXmlPacker ddzToXmlPacker;
    private final SignPlainTextBuilder signPlainTextBuilder;

    public MessageTransformResult xmlToDdz(String wkeCode, String xml) {
        List<RspCfg> cfgList = loadCfg(wkeCode);
        Document doc = parseXml(xml);
        Element root = doc.getRootElement();

        TransformDefinitionResolver.TransformMeta meta = definitionResolver.resolve(cfgList);
        Map<String, Object> dtoMap = xmlToDdzUnpacker.unpack(root, meta.getDefs());
        String signPlainText = signPlainTextBuilder.build(cfgList, root);

        return MessageTransformResult.builder()
                .dtoMap(dtoMap)
                .xml(xml)
                .signPlainText(signPlainText)
                .build();
    }

    public MessageTransformResult ddzToXml(String wkeCode, Map<String, Object> dtoMap) {
        List<RspCfg> cfgList = loadCfg(wkeCode);
        TransformDefinitionResolver.TransformMeta meta = definitionResolver.resolve(cfgList);

        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement(meta.getRootName());
        ddzToXmlPacker.pack(root, dtoMap, meta.getDefs(), meta.getChildOrderIndex());

        String xml = toPrettyXml(doc);
        String signPlainText = signPlainTextBuilder.build(cfgList, root);

        return MessageTransformResult.builder()
                .dtoMap(dtoMap)
                .xml(xml)
                .signPlainText(signPlainText)
                .build();
    }

    private List<RspCfg> loadCfg(String wkeCode) {
        List<RspCfg> cfg = rspCfgMapper.selectByWkeCode(wkeCode);
        if (cfg == null || cfg.isEmpty()) {
            throw new IllegalArgumentException("rsp_cfg_t未找到wke_code=" + wkeCode + "的配置");
        }
        return cfg;
    }

    private Document parseXml(String xml) {
        try {
            SAXReader reader = new SAXReader();
            return reader.read(new StringReader(xml));
        } catch (Exception e) {
            throw new IllegalArgumentException("XML解析失败: " + e.getMessage(), e);
        }
    }

    private String toPrettyXml(Document doc) {
        try {
            OutputFormat format = OutputFormat.createPrettyPrint();
            format.setEncoding("UTF-8");
            StringWriter sw = new StringWriter();
            XMLWriter writer = new XMLWriter(sw, format);
            writer.write(doc);
            writer.flush();
            return sw.toString();
        } catch (Exception e) {
            throw new IllegalStateException("XML输出失败", e);
        }
    }
}
