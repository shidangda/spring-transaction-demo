package com.example.service.message.transform;

import com.example.entity.RspCfg;
import com.example.service.message.transform.TransformDefinitionResolver.DdzDef;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.dom4j.Element;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class XmlToDdzUnpacker {

    public Map<String, Object> unpack(Element root, Map<String, DdzDef> defs) {
        Map<String, List<DdzInstance>> parsed = new LinkedHashMap<>();
        for (DdzDef def : defs.values()) {
            parsed.put(def.getDdzName(), parseInstancesFromXml(root, def));
        }

        linkParentChild(parsed, defs);

        Map<String, Object> dtoMap = new LinkedHashMap<>();
        for (DdzDef def : defs.values()) {
            if (def.getParentDdzName() != null) {
                continue;
            }
            List<Map<String, Object>> list = parsed.getOrDefault(def.getDdzName(), Collections.emptyList())
                    .stream().map(DdzInstance::getData).collect(Collectors.toList());
            if (!list.isEmpty()) {
                dtoMap.put(def.getDdzName(), list);
            }
        }
        return dtoMap;
    }

    private List<DdzInstance> parseInstancesFromXml(Element root, DdzDef def) {
        List<Element> units = XmlPathSupport.selectElements(root, def.getUnitPathSegments().subList(1, def.getUnitPathSegments().size()));
        List<DdzInstance> out = new ArrayList<>();

        for (Element unit : units) {
            Map<String, Object> item = new LinkedHashMap<>();
            for (RspCfg row : def.getRows()) {
                String fieldName = row.getFieldName();
                if (fieldName == null || fieldName.trim().isEmpty()) {
                    continue;
                }

                List<String> full = XmlPathSupport.splitPath(row.getXpath());
                String value;
                if (def.getRows().size() == 1 && full.equals(def.getUnitPathSegments())) {
                    value = unit.getTextTrim();
                } else {
                    List<String> relative = full.subList(def.getUnitPathSegments().size(), full.size());
                    Element leaf = XmlPathSupport.getByRelativePath(unit, relative);
                    value = leaf == null ? null : leaf.getTextTrim();
                }

                if (value != null) {
                    item.put(fieldName, value);
                }
            }
            out.add(new DdzInstance(unit, item));
        }
        return out;
    }

    private void linkParentChild(Map<String, List<DdzInstance>> parsed, Map<String, DdzDef> defs) {
        for (DdzDef child : defs.values()) {
            if (child.getParentDdzName() == null) {
                continue;
            }
            DdzDef parent = defs.get(child.getParentDdzName());
            List<DdzInstance> childInstances = parsed.getOrDefault(child.getDdzName(), Collections.emptyList());
            List<DdzInstance> parentInstances = parsed.getOrDefault(parent.getDdzName(), Collections.emptyList());

            for (DdzInstance c : childInstances) {
                DdzInstance holder = null;
                for (DdzInstance p : parentInstances) {
                    if (isAncestor(p.getUnitElement(), c.getUnitElement())) {
                        holder = p;
                        break;
                    }
                }
                if (holder != null) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> list = (List<Map<String, Object>>) holder.getData()
                            .computeIfAbsent(child.getDdzName(), k -> new ArrayList<Map<String, Object>>());
                    list.add(c.getData());
                }
            }
        }
    }

    private boolean isAncestor(Element ancestor, Element child) {
        Element p = child;
        while (p != null) {
            if (p == ancestor) {
                return true;
            }
            p = p.getParent();
        }
        return false;
    }

    @Data
    @AllArgsConstructor
    private static class DdzInstance {
        private Element unitElement;
        private Map<String, Object> data;
    }
}
