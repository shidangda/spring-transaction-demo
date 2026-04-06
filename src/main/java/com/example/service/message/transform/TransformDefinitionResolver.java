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

            // 有声明子对象的 DDZ 视作容器，优先向结构锚点回退 unitPath。
            boolean preferStructureAnchor = !def.getDeclaredChildren().isEmpty();
            def.setUnitPathSegments(inferUnitPath(rows, structureAnchors, preferStructureAnchor));
            def.setMulti(resolveDefMulti(def, structureAnchors));
            defs.put(ddzName, def);
        }

        // 先算“路径上的候选父”（最长前缀）
        Map<String, String> candidateParent = resolveCandidateParents(defs);

        // 真正生效的父子关系：必须满足“父 DDZ 在代码定义里声明了该子 DDZ”。
        for (DdzDef child : defs.values()) {
            String candidate = candidateParent.get(child.getDdzName());
            if (candidate == null) {
                continue;
            }
            DdzDef parent = defs.get(candidate);
            if (parent != null && parent.getDeclaredChildren().contains(child.getDdzName())) {
                child.setParentDdzName(parent.getDdzName());
                child.setParentUnitPathSegments(parent.getUnitPathSegments());
            }
        }

        validateConfigCompatibility(defs, candidateParent, structureAnchors);

        String rootName = firstSegment(cfgList.get(0).getXpath());
        Map<String, Integer> childOrderIndex = buildChildOrderIndex(cfgList);
        return new TransformMeta(defs, structureAnchors, rootName, childOrderIndex);
    }

    private Map<String, String> resolveCandidateParents(Map<String, DdzDef> defs) {
        Map<String, String> out = new LinkedHashMap<>();
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
            out.put(child.getDdzName(), bestParent == null ? null : bestParent.getDdzName());
        }
        return out;
    }

    /**
     * 配置等价性校验：
     * 1) 若代码声明了子 DDZ，但配置中不存在该 DDZ，报错；
     * 2) 若某 DDZ 位于多包锚点下，且该锚点由某候选父 DDZ 承担，但父 DDZ 未声明该子 DDZ，报错。
     */
    private void validateConfigCompatibility(Map<String, DdzDef> defs,
                                             Map<String, String> candidateParent,
                                             List<StructureAnchor> anchors) {
        List<String> errors = new ArrayList<>();

        for (DdzDef parent : defs.values()) {
            for (String childName : parent.getDeclaredChildren()) {
                if (!defs.containsKey(childName)) {
                    errors.add("DDZ[" + parent.getDdzName() + "] 在代码中声明子对象 [" + childName + "]，但 XML 配置中不存在该 ddz_name");
                }
            }
        }

        for (DdzDef child : defs.values()) {
            List<String> nearestMultiAnchor = findNearestMultiAnchor(child.getUnitPathSegments(), anchors);
            if (nearestMultiAnchor.isEmpty()) {
                continue;
            }

            String parentName = candidateParent.get(child.getDdzName());
            if (parentName == null) {
                continue;
            }

            DdzDef parent = defs.get(parentName);
            if (parent == null) {
                continue;
            }

            // 仅在“候选父正好承载该多包锚点”时要求代码声明该子对象。
            if (Objects.equals(parent.getUnitPathSegments(), nearestMultiAnchor)
                    && !parent.getDeclaredChildren().contains(child.getDdzName())) {
                errors.add("DDZ[" + child.getDdzName() + "] 位于多包结构 ["
                        + joinPath(nearestMultiAnchor)
                        + "] 下，候选父 DDZ[" + parent.getDdzName()
                        + "] 未在代码中声明该子对象，无法保证 XML<->DDZ 等价转换");
            }
        }

        if (!errors.isEmpty()) {
            throw new IllegalArgumentException("XML配置与DDZ接口定义不兼容:\n - " + String.join("\n - ", errors));
        }
    }

    private List<String> findNearestMultiAnchor(List<String> path, List<StructureAnchor> anchors) {
        List<String> best = Collections.emptyList();
        int bestLen = -1;
        for (StructureAnchor anchor : anchors) {
            if (!anchor.isMulti()) {
                continue;
            }
            List<String> ap = anchor.getPath();
            if (ap.size() >= path.size()) {
                continue;
            }
            if (XmlPathSupport.isPrefix(ap, path) && ap.size() > bestLen) {
                best = ap;
                bestLen = ap.size();
            }
        }
        return best;
    }

    private String joinPath(List<String> path) {
        return "/" + String.join("/", path);
    }

    private Map<String, Integer> buildChildOrderIndex(List<RspCfg> cfgList) {
        Map<String, Integer> out = new LinkedHashMap<>();
        List<RspCfg> sorted = cfgList.stream()
                .sorted(Comparator.comparing(RspCfg::getFieldSeq, Comparator.nullsLast(Integer::compareTo)))
                .collect(Collectors.toList());
        for (RspCfg row : sorted) {
            List<String> path = XmlPathSupport.splitPath(row.getXpath());
            if (path.size() < 2) {
                continue;
            }
            List<String> parent = path.subList(0, path.size() - 1);
            String child = path.get(path.size() - 1);
            String key = pathKey(parent) + ">" + child;
            if (!out.containsKey(key) && row.getFieldSeq() != null) {
                out.put(key, row.getFieldSeq());
            }
        }
        return out;
    }

    private String pathKey(List<String> path) {
        return "/" + String.join("/", path);
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
            // 允许“等长锚点”命中：当 base 本身就是结构节点时，不应错误回退到更上层（如根节点）。
            if (sp.size() <= base.size() && XmlPathSupport.isPrefix(sp, base) && sp.size() > bestLen) {
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
        private final Map<String, Integer> childOrderIndex;
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
