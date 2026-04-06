package com.example.service.message.transform;

import com.example.entity.RspCfg;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class TransformDefinitionResolver {

    public TransformMeta resolve(List<RspCfg> cfgList) {
        Map<String, List<RspCfg>> byDdz = cfgList.stream()
                .filter(c -> c.getDdzName() != null && !c.getDdzName().trim().isEmpty())
                .collect(Collectors.groupingBy(RspCfg::getDdzName, LinkedHashMap::new, Collectors.toList()));

        List<StructureAnchor> structureAnchors = cfgList.stream()
                .filter(c -> c.getDdzName() == null || c.getDdzName().trim().isEmpty())
                .map(c -> new StructureAnchor(XmlPathSupport.splitPath(c.getXpath()), isMultiTag(c.getMultiTag())))
                .filter(a -> !a.getPath().isEmpty())
                .collect(Collectors.toList());

        Map<String, DdzDef> defs = new LinkedHashMap<>();
        for (Map.Entry<String, List<RspCfg>> e : byDdz.entrySet()) {
            String ddzName = e.getKey();
            List<RspCfg> rows = e.getValue().stream()
                    .sorted(Comparator.comparing(RspCfg::getFieldSeq, Comparator.nullsLast(Integer::compareTo)))
                    .collect(Collectors.toList());

            DdzDef def = new DdzDef();
            def.setDdzName(ddzName);
            def.setRows(rows);
            def.setDeclaredChildren(resolveDeclaredChildDdzNames(ddzName));

            boolean preferStructureAnchor = !def.getDeclaredChildren().isEmpty();
            def.setUnitPathSegments(inferUnitPath(rows, structureAnchors, preferStructureAnchor));
            def.setMulti(resolveDefMulti(def, structureAnchors));
            defs.put(ddzName, def);
        }

        for (DdzDef child : defs.values()) {
            DdzDef bestParent = null;
            int bestLen = -1;
            for (DdzDef parent : defs.values()) {
                if (parent == child) {
                    continue;
                }
                if (XmlPathSupport.isPrefix(parent.getUnitPathSegments(), child.getUnitPathSegments())
                        && parent.getUnitPathSegments().size() < child.getUnitPathSegments().size()
                        && parent.getUnitPathSegments().size() > bestLen) {
                    bestParent = parent;
                    bestLen = parent.getUnitPathSegments().size();
                }
            }
            if (bestParent != null) {
                child.setParentDdzName(bestParent.getDdzName());
                child.setParentUnitPathSegments(bestParent.getUnitPathSegments());
            }
        }

        String rootName = firstSegment(cfgList.get(0).getXpath());
        return new TransformMeta(defs, structureAnchors, rootName);
    }

    private List<String> inferUnitPath(List<RspCfg> rows, List<StructureAnchor> structureAnchors, boolean preferStructureAnchor) {
        List<String> base;
        if (rows.size() == 1) {
            base = XmlPathSupport.splitPath(rows.get(0).getXpath());
        } else {
            List<String> lcp = XmlPathSupport.splitPath(rows.get(0).getXpath());
            for (int i = 1; i < rows.size(); i++) {
                lcp = XmlPathSupport.commonPrefix(lcp, XmlPathSupport.splitPath(rows.get(i).getXpath()));
            }
            base = lcp;
        }

        if (!preferStructureAnchor) {
            return base;
        }

        List<String> best = Collections.emptyList();
        int bestLen = -1;
        for (StructureAnchor anchor : structureAnchors) {
            List<String> sp = anchor.getPath();
            if (sp.size() < base.size() && XmlPathSupport.isPrefix(sp, base) && sp.size() > bestLen) {
                best = sp;
                bestLen = sp.size();
            }
        }
        return best.isEmpty() ? base : best;
    }

    private boolean resolveDefMulti(DdzDef def, List<StructureAnchor> structureAnchors) {
        for (StructureAnchor anchor : structureAnchors) {
            if (Objects.equals(anchor.getPath(), def.getUnitPathSegments())) {
                return anchor.isMulti();
            }
        }
        return def.getRows().stream().anyMatch(r -> isMultiTag(r.getMultiTag()));
    }

    private Set<String> resolveDeclaredChildDdzNames(String ddzName) {
        Set<String> out = new LinkedHashSet<>();
        try {
            Class<?> clazz = Class.forName("com.example.entity.ddz." + ddzName);
            for (Field f : clazz.getDeclaredFields()) {
                Type genericType = f.getGenericType();
                if (!(genericType instanceof ParameterizedType)) {
                    continue;
                }
                ParameterizedType pt = (ParameterizedType) genericType;
                Type rawType = pt.getRawType();
                if (!(rawType instanceof Class) || !List.class.isAssignableFrom((Class<?>) rawType)) {
                    continue;
                }
                Type[] args = pt.getActualTypeArguments();
                if (args.length != 1 || !(args[0] instanceof Class)) {
                    continue;
                }
                Class<?> argClass = (Class<?>) args[0];
                if (argClass.getPackage() != null
                        && "com.example.entity.ddz".equals(argClass.getPackage().getName())) {
                    out.add(argClass.getSimpleName());
                }
            }
        } catch (Exception ignore) {
        }
        return out;
    }

    private String firstSegment(String xpath) {
        List<String> segs = XmlPathSupport.splitPath(xpath);
        if (segs.isEmpty()) {
            throw new IllegalArgumentException("非法xpath: " + xpath);
        }
        return segs.get(0);
    }

    private boolean isMultiTag(String multiTag) {
        return multiTag != null && "M".equalsIgnoreCase(multiTag.trim());
    }

    @Data
    public static class TransformMeta {
        private final Map<String, DdzDef> defs;
        private final List<StructureAnchor> structureAnchors;
        private final String rootName;
    }

    @Data
    public static class DdzDef {
        private String ddzName;
        private List<RspCfg> rows;
        private List<String> unitPathSegments;
        private Set<String> declaredChildren = new LinkedHashSet<>();
        private String parentDdzName;
        private List<String> parentUnitPathSegments;
        private boolean multi;
    }

    @Data
    public static class StructureAnchor {
        private final List<String> path;
        private final boolean multi;
    }
}
