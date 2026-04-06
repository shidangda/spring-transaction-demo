package com.example.service.message.transform;

import com.example.entity.RspCfg;
import com.example.service.message.transform.TransformDefinitionResolver.DdzDef;
import lombok.RequiredArgsConstructor;
import org.dom4j.Element;
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

    public void pack(Element root, Map<String, Object> dtoMap, Map<String, DdzDef> defs) {
        for (DdzDef def : defs.values()) {
            if (def.getParentDdzName() != null) {
                continue;
            }
            List<Map<String, Object>> instances = castInstanceList(dtoMap.get(def.getDdzName()));
            if (!def.isMulti() && !instances.isEmpty()) {
                writeDdzInstance(root, def, instances.get(0), defs);
                continue;
            }
            for (Map<String, Object> instance : instances) {
                writeDdzInstance(root, def, instance, defs);
            }
        }
    }

    private void writeDdzInstance(Element root, DdzDef def, Map<String, Object> instance, Map<String, DdzDef> defs) {
        Element unit = createUnitElement(root, def.getUnitPathSegments());
        writeDdzUnitByFieldSeq(unit, def, instance, defs);
    }

    private void writeDdzChildInstance(Element parentUnit, DdzDef childDef, Map<String, Object> childInstance, Map<String, DdzDef> defs) {
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

        writeDdzUnitByFieldSeq(cursor, childDef, childInstance, defs);
    }

    private void writeDdzUnitByFieldSeq(Element unit, DdzDef def, Map<String, Object> instance, Map<String, DdzDef> defs) {
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
            ops.add(SeqWriteOp.child(child));
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
                writeDdzChildInstance(unit, child, childInstances.get(0), defs);
                continue;
            }
            for (Map<String, Object> childInstance : childInstances) {
                writeDdzChildInstance(unit, child, childInstance, defs);
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

        boolean isMulti = isMultiTag(row.getMultiTag());
        writeLeafByRelativePath(unit, relative, value, isMulti);
    }

    private void writeLeafByRelativePath(Element unit, List<String> relative, Object value, boolean multi) {
        if (relative == null || relative.isEmpty()) {
            return;
        }

        Element cursor = unit;
        for (int i = 0; i < relative.size() - 1; i++) {
            String seg = relative.get(i);
            Element next = cursor.element(seg);
            if (next == null) {
                next = cursor.addElement(seg);
            }
            cursor = next;
        }

        String leafName = relative.get(relative.size() - 1);
        if (multi) {
            List<Object> values = flattenToValues(value);
            for (Object one : values) {
                if (one == null) {
                    continue;
                }
                cursor.addElement(leafName).setText(String.valueOf(one));
            }
            return;
        }

        Element leaf = cursor.element(leafName);
        if (leaf == null) {
            leaf = cursor.addElement(leafName);
        }
        leaf.setText(String.valueOf(value));
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

    private Element createUnitElement(Element root, List<String> fullPath) {
        List<String> relative = fullPath.subList(1, fullPath.size());
        Element cursor = root;
        for (int i = 0; i < relative.size(); i++) {
            String seg = relative.get(i);
            boolean isLast = i == relative.size() - 1;
            if (isLast) {
                cursor = cursor.addElement(seg);
            } else {
                Element next = cursor.element(seg);
                if (next == null) {
                    next = cursor.addElement(seg);
                }
                cursor = next;
            }
        }
        return cursor;
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

        private SeqWriteOp(RspCfg fieldRow, DdzDef childDef) {
            this.fieldRow = fieldRow;
            this.childDef = childDef;
        }

        private static SeqWriteOp field(RspCfg row) {
            return new SeqWriteOp(row, null);
        }

        private static SeqWriteOp child(DdzDef def) {
            return new SeqWriteOp(null, def);
        }

        private int seq() {
            if (fieldRow != null) {
                return fieldRow.getFieldSeq() == null ? Integer.MAX_VALUE : fieldRow.getFieldSeq();
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
    }
}
