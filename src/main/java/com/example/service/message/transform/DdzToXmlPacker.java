package com.example.service.message.transform;

import com.example.entity.RspCfg;
import com.example.service.message.transform.TransformDefinitionResolver.DdzDef;
import lombok.RequiredArgsConstructor;
import org.dom4j.Element;
import org.dom4j.Node;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
@RequiredArgsConstructor
public class DdzToXmlPacker {

    public void pack(Element root, Map<String, Object> dtoMap, Map<String, DdzDef> defs, Map<String, Integer> childOrderIndex) {
        for (DdzDef def : defs.values()) {
            if (def.getParentDdzName() != null) {
                continue;
            }
            List<Map<String, Object>> instances = castInstanceList(dtoMap.get(def.getDdzName()));
            if (!def.isMulti() && !instances.isEmpty()) {
                writeDdzInstance(root, def, instances.get(0), defs, childOrderIndex);
                continue;
            }
            for (Map<String, Object> instance : instances) {
                writeDdzInstance(root, def, instance, defs, childOrderIndex);
            }
        }
    }

    private void writeDdzInstance(Element root, DdzDef def, Map<String, Object> instance, Map<String, DdzDef> defs, Map<String, Integer> childOrderIndex) {
        Element unit = createUnitElement(root, def.getUnitPathSegments(), childOrderIndex);
        writeDdzUnitByFieldSeq(unit, def, instance, defs, childOrderIndex);
    }

    private void writeDdzChildInstance(Element parentUnit, DdzDef childDef, Map<String, Object> childInstance, Map<String, DdzDef> defs, Map<String, Integer> childOrderIndex) {
        List<String> parentPath = childDef.getParentUnitPathSegments();
        List<String> childPath = childDef.getUnitPathSegments();
        List<String> relative = childPath.subList(parentPath.size(), childPath.size());

        Element cursor = parentUnit;
        for (int i = 0; i < relative.size(); i++) {
            String seg = relative.get(i);
            boolean isLeafUnit = i == relative.size() - 1;
            if (isLeafUnit) {
                cursor = cursor.addElement(seg);
            } else {
                Element next = cursor.element(seg);
                if (next == null) {
                    next = cursor.addElement(seg);
                }
                cursor = next;
            }
        }

        writeDdzUnitByFieldSeq(cursor, childDef, childInstance, defs, childOrderIndex);
    }

    private void writeDdzUnitByFieldSeq(Element unit, DdzDef def, Map<String, Object> instance, Map<String, DdzDef> defs, Map<String, Integer> childOrderIndex) {
        List<SeqWriteOp> ops = new ArrayList<>();

        for (RspCfg row : def.getRows()) {
            if (row.getFieldName() == null || row.getFieldName().trim().isEmpty()) {
                continue;
            }
            ops.add(SeqWriteOp.field(row));
        }

        for (DdzDef child : defs.values()) {
            if (!def.getDdzName().equals(child.getParentDdzName())) {
                continue;
            }
            ops.add(SeqWriteOp.child(def, child, childOrderIndex));
        }

        ops.sort(Comparator.comparingInt(SeqWriteOp::seq));

        for (SeqWriteOp op : ops) {
            if (op.fieldRow != null) {
                writeFieldByCfg(unit, def, instance, op.fieldRow);
                continue;
            }

            DdzDef child = op.childDef;
            List<Map<String, Object>> childInstances = castInstanceList(instance.get(child.getDdzName()));
            if (!child.isMulti() && !childInstances.isEmpty()) {
                writeDdzChildInstance(unit, child, childInstances.get(0), defs, childOrderIndex);
                continue;
            }
            for (Map<String, Object> childInstance : childInstances) {
                writeDdzChildInstance(unit, child, childInstance, defs, childOrderIndex);
            }
        }
    }

    private void writeFieldByCfg(Element unit, DdzDef def, Map<String, Object> instance, RspCfg row) {
        String fieldName = row.getFieldName();
        Object value = instance.get(fieldName);
        if (value == null) {
            return;
        }

        List<String> fullPath = XmlPathSupport.splitPath(row.getXpath());
        if (fullPath.equals(def.getUnitPathSegments()) && def.getRows().size() == 1) {
            unit.setText(String.valueOf(value));
            return;
        }

        List<String> relative = fullPath.subList(def.getUnitPathSegments().size(), fullPath.size());
        if (relative.isEmpty()) {
            return;
        }

        // 特殊映射：.../SttlmPrty-/A03 -> <SttlmPrty>/A03/{value}</SttlmPrty>
        if (relative.size() >= 2 && XmlPathSupport.isTaggedContainerSegment(relative.get(0))) {
            writeTaggedField(unit, relative, value);
            return;
        }

        boolean isMulti = isMultiTag(row.getMultiTag());
        writeValueByRelativePath(unit, relative, value, isMulti);
    }

    private void writeValueByRelativePath(Element unit, List<String> relative, Object value, boolean multi) {
        if (relative == null || relative.isEmpty()) {
            return;
        }

        Element cursor = unit;
        for (int i = 0; i < relative.size() - 1; i++) {
            String seg = relative.get(i);
            if (XmlPathSupport.isAttributeSegment(seg)) {
                // 中间段不应是属性，忽略非法配置
                return;
            }
            Element next = cursor.element(seg);
            if (next == null) {
                next = cursor.addElement(seg);
            }
            cursor = next;
        }

        String lastSeg = relative.get(relative.size() - 1);
        if (XmlPathSupport.isAttributeSegment(lastSeg)) {
            String attrName = XmlPathSupport.attributeName(lastSeg);
            if (multi) {
                List<Object> values = flattenToValues(value);
                if (!values.isEmpty() && values.get(0) != null) {
                    cursor.addAttribute(attrName, String.valueOf(values.get(0)));
                }
            } else {
                cursor.addAttribute(attrName, String.valueOf(value));
            }
            return;
        }

        if (multi) {
            List<Object> values = flattenToValues(value);
            for (Object one : values) {
                if (one == null) {
                    continue;
                }
                cursor.addElement(lastSeg).setText(String.valueOf(one));
            }
            return;
        }

        Element leaf = cursor.element(lastSeg);
        if (leaf == null) {
            leaf = cursor.addElement(lastSeg);
        }
        leaf.setText(String.valueOf(value));
    }

    private void writeTaggedField(Element unit, List<String> relative, Object value) {
        String containerSeg = relative.get(0);
        String tagCode = relative.get(1);
        if (tagCode == null || tagCode.trim().isEmpty()) {
            return;
        }

        String elementName = XmlPathSupport.taggedElementName(containerSeg);
        if (elementName == null || elementName.trim().isEmpty()) {
            return;
        }

        Element container = unit.element(elementName);
        if (container == null) {
            container = unit.addElement(elementName);
        }

        // 配置约定把 DDZ 字段值映射到 /A03/{值} 的 {值} 段
        container.setText("/" + tagCode + "/" + String.valueOf(value));
    }

    private List<Object> flattenToValues(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }
        if (value instanceof Collection) {
            return new ArrayList<>((Collection<?>) value);
        }
        Class<?> clazz = value.getClass();
        if (clazz.isArray()) {
            int len = java.lang.reflect.Array.getLength(value);
            List<Object> out = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                out.add(java.lang.reflect.Array.get(value, i));
            }
            return out;
        }
        return Collections.singletonList(value);
    }

    private boolean isMultiTag(String multiTag) {
        return multiTag != null && "M".equalsIgnoreCase(multiTag.trim());
    }

    private Element createUnitElement(Element root, List<String> fullPath, Map<String, Integer> childOrderIndex) {
        List<String> relative = fullPath.subList(1, fullPath.size());
        Element cursor = root;
        List<String> currentPath = new ArrayList<>();
        currentPath.add(root.getName());

        for (int i = 0; i < relative.size(); i++) {
            String seg = relative.get(i);
            boolean isLast = i == relative.size() - 1;

            if (isLast) {
                // unit 本身也要按全局顺序插入，避免跨 DDZ 的同层顺序错位
                cursor = addElementByGlobalOrder(cursor, currentPath, seg, childOrderIndex);
            } else {
                Element next = cursor.element(seg);
                if (next == null) {
                    next = addElementByGlobalOrder(cursor, currentPath, seg, childOrderIndex);
                }
                cursor = next;
                currentPath.add(seg);
            }
        }
        return cursor;
    }

    private Element addElementByGlobalOrder(Element parent,
                                            List<String> parentPath,
                                            String childName,
                                            Map<String, Integer> childOrderIndex) {
        if (childOrderIndex == null || childOrderIndex.isEmpty()) {
            return parent.addElement(childName);
        }

        String parentKey = "/" + String.join("/", parentPath);
        String selfKey = parentKey + ">" + childName;
        Integer selfSeq = childOrderIndex.get(selfKey);

        @SuppressWarnings("unchecked")
        List<Element> siblings = parent.elements();
        int insertAt = siblings.size();
        if (selfSeq != null) {
            for (int i = 0; i < siblings.size(); i++) {
                Element sib = siblings.get(i);
                Integer sibSeq = childOrderIndex.get(parentKey + ">" + sib.getName());
                if (sibSeq != null && sibSeq > selfSeq) {
                    insertAt = i;
                    break;
                }
            }
        }

        Element newChild = parent.addElement(childName);
        if (insertAt < siblings.size()) {
            @SuppressWarnings("unchecked")
            List<Node> content = parent.content();
            if (!content.isEmpty()) {
                Node justAdded = content.remove(content.size() - 1);
                int targetIndex = 0;
                int seenElements = 0;
                for (; targetIndex < content.size(); targetIndex++) {
                    Node node = content.get(targetIndex);
                    if (node instanceof Element) {
                        if (seenElements == insertAt) {
                            break;
                        }
                        seenElements++;
                    }
                }
                content.add(targetIndex, justAdded);
            }
        }
        return newChild;
    }

    private List<Map<String, Object>> castInstanceList(Object v) {
        if (v == null) {
            return Collections.emptyList();
        }
        if (!(v instanceof List)) {
            return Collections.emptyList();
        }

        List<?> raw = (List<?>) v;
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object o : raw) {
            if (o instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> one = (Map<String, Object>) o;
                out.add(one);
            }
        }
        return out;
    }

    private static class SeqWriteOp {
        private final RspCfg fieldRow;
        private final DdzDef childDef;
        private final Integer childOrderSeq;

        private SeqWriteOp(RspCfg fieldRow, DdzDef childDef, Integer childOrderSeq) {
            this.fieldRow = fieldRow;
            this.childDef = childDef;
            this.childOrderSeq = childOrderSeq;
        }

        private static SeqWriteOp field(RspCfg row) {
            return new SeqWriteOp(row, null, null);
        }

        private static SeqWriteOp child(DdzDef parent, DdzDef child, Map<String, Integer> childOrderIndex) {
            Integer seq = resolveChildSeq(parent, child, childOrderIndex);
            return new SeqWriteOp(null, child, seq);
        }

        private int seq() {
            if (fieldRow != null) {
                return fieldRow.getFieldSeq() == null ? Integer.MAX_VALUE : fieldRow.getFieldSeq();
            }
            if (childOrderSeq != null) {
                return childOrderSeq;
            }
            if (childDef == null || childDef.getRows() == null || childDef.getRows().isEmpty()) {
                return Integer.MAX_VALUE;
            }
            return childDef.getRows().stream()
                    .map(RspCfg::getFieldSeq)
                    .filter(Objects::nonNull)
                    .min(Integer::compareTo)
                    .orElse(Integer.MAX_VALUE);
        }

        private static Integer resolveChildSeq(DdzDef parent, DdzDef child, Map<String, Integer> childOrderIndex) {
            if (parent == null || child == null || childOrderIndex == null) {
                return null;
            }
            List<String> parentPath = parent.getUnitPathSegments();
            List<String> childPath = child.getUnitPathSegments();
            if (parentPath == null || childPath == null || childPath.size() <= parentPath.size()) {
                return null;
            }
            String childNodeName = childPath.get(parentPath.size());
            String key = pathKey(parentPath) + ">" + childNodeName;
            return childOrderIndex.get(key);
        }

        private static String pathKey(List<String> path) {
            return "/" + String.join("/", path);
        }
    }
}
