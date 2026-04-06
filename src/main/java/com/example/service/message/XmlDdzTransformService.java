package com.example.service.message;

import com.example.entity.RspCfg;
import com.example.mapper.RspCfgMapper;
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
import java.util.*;
import java.util.stream.Collectors;

/**
 * XML 与 DDZ 结构互转核心服务。
 *
 * 设计思路（基于 rsp_cfg_t）：
 * 1) 按 ddz_name 对配置分组，得到每个 DDZ 对象的字段定义。
 * 2) 用 xpath 的最长公共前缀推导“对象单元路径(unitPath)”；
 *    - 单字段对象：unitPath 即该字段 xpath；
 *    - 多字段对象：unitPath 为多个字段 xpath 的公共前缀。
 * 3) 通过 unitPath 的前缀关系推断父子 DDZ（嵌套）。
 * 4) XML -> DDZ：先解析每个 DDZ 的实例，再根据 XML 节点祖先关系回挂子对象。
 * 5) DDZ -> XML：从根 DDZ 开始递归写出节点与字段。
 */
@Service
@RequiredArgsConstructor
public class XmlDdzTransformService {

    private final RspCfgMapper rspCfgMapper;

    /**
     * XML 转 DDZ。
     * 返回值里同时带上签名原文串（按 field_seq 顺序拼接）。
     */
    public MessageTransformResult xmlToDdz(String wkeCode, String xml) {
        // 1) 加载配置并解析 XML
        List<RspCfg> cfgList = loadCfg(wkeCode);
        Document doc = parseXml(xml);
        Element root = doc.getRootElement();

        // 2) 计算 DDZ 定义：字段、unitPath、父子关系
        Map<String, DdzDef> defs = buildDefs(cfgList);

        // 3) 分 DDZ 解析实例（先“平铺”解析，不做父子挂接）
        Map<String, List<DdzInstance>> parsed = new LinkedHashMap<>();
        for (DdzDef def : defs.values()) {
            parsed.put(def.ddzName, parseInstancesFromXml(root, def));
        }

        // 4) 基于节点祖先关系，将子 DDZ 挂到父 DDZ
        linkParentChild(parsed, defs);

        // 5) 只输出根 DDZ（父为空），子 DDZ 已被挂入父对象内部
        Map<String, Object> dtoMap = new LinkedHashMap<>();
        for (DdzDef def : defs.values()) {
            if (def.parentDdzName != null) {
                continue;
            }
            List<Map<String, Object>> list = parsed.getOrDefault(def.ddzName, Collections.emptyList())
                    .stream().map(ins -> ins.data).collect(Collectors.toList());
            if (!list.isEmpty()) {
                dtoMap.put(def.ddzName, list);
            }
        }

        String signPlainText = buildSignPlainText(cfgList, root);
        return MessageTransformResult.builder()
                .dtoMap(dtoMap)
                .xml(xml)
                .signPlainText(signPlainText)
                .build();
    }

    /**
     * DDZ 转 XML。
     * dtoMap 结构要求与 XMLCONFIG.json 同型：
     * { "X1": [ {...}, {...} ], "X2": [ ... ] }
     */
    public MessageTransformResult ddzToXml(String wkeCode, Map<String, Object> dtoMap) {
        // 1) 加载配置并构建 DDZ 定义
        List<RspCfg> cfgList = loadCfg(wkeCode);
        Map<String, DdzDef> defs = buildDefs(cfgList);

        // 2) 根节点取第一条 xpath 的首段（例如 Document）
        String rootName = firstSegment(cfgList.get(0).getXpath());
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement(rootName);

        // 3) 从根 DDZ 开始递归写入 XML
        for (DdzDef def : defs.values()) {
            if (def.parentDdzName != null) {
                continue;
            }
            List<Map<String, Object>> instances = castInstanceList(dtoMap.get(def.ddzName));
            for (Map<String, Object> instance : instances) {
                writeDdzInstance(root, def, instance, defs);
            }
        }

        String xml = toPrettyXml(doc);
        String signPlainText = buildSignPlainText(cfgList, root);

        return MessageTransformResult.builder()
                .dtoMap(dtoMap)
                .xml(xml)
                .signPlainText(signPlainText)
                .build();
    }

    /**
     * 写一个“根级”DDZ 实例到 XML。
     * 该方法负责：
     * - 创建本 DDZ 的 unit 节点
     * - 写本 DDZ 字段
     * - 递归写子 DDZ
     */
    private void writeDdzInstance(Element root, DdzDef def, Map<String, Object> instance, Map<String, DdzDef> defs) {
        // 根据 unitPath 在 XML 上创建该实例对应的“对象单元节点”
        Element unit = createUnitElement(root, def.unitPathSegments);

        // 写当前 DDZ 的字段
        for (RspCfg row : def.rows) {
            String fieldName = row.getFieldName();
            if (fieldName == null || fieldName.trim().isEmpty()) {
                continue;
            }
            Object value = instance.get(fieldName);
            if (value == null) {
                continue;
            }

            List<String> fullPath = splitPath(row.getXpath());

            // 特殊场景：单字段对象且 xpath 就是 unitPath，直接写节点文本
            if (fullPath.equals(def.unitPathSegments) && def.rows.size() == 1) {
                unit.setText(String.valueOf(value));
                continue;
            }

            // 其余场景：在 unit 下按相对路径补齐中间层并写叶子
            List<String> relative = fullPath.subList(def.unitPathSegments.size(), fullPath.size());
            if (relative.isEmpty()) {
                continue;
            }

            Element cursor = unit;
            for (int i = 0; i < relative.size(); i++) {
                String seg = relative.get(i);
                boolean isLeaf = i == relative.size() - 1;
                if (isLeaf) {
                    Element leaf = cursor.addElement(seg);
                    leaf.setText(String.valueOf(value));
                } else {
                    Element next = cursor.element(seg);
                    if (next == null) {
                        next = cursor.addElement(seg);
                    }
                    cursor = next;
                }
            }
        }

        // 递归写子 DDZ
        for (Map.Entry<String, DdzDef> entry : defs.entrySet()) {
            DdzDef child = entry.getValue();
            if (!def.ddzName.equals(child.parentDdzName)) {
                continue;
            }
            List<Map<String, Object>> childInstances = castInstanceList(instance.get(child.ddzName));
            for (Map<String, Object> childInstance : childInstances) {
                writeDdzChildInstance(unit, child, childInstance, defs);
            }
        }
    }

    /**
     * 写子 DDZ 实例。
     * parentUnit 为父 DDZ 当前实例的 unit 节点。
     */
    private void writeDdzChildInstance(Element parentUnit, DdzDef childDef, Map<String, Object> childInstance, Map<String, DdzDef> defs) {
        List<String> parentPath = childDef.parentUnitPathSegments;
        List<String> childPath = childDef.unitPathSegments;

        // 仅在父 unit 下补 child 相对段，避免重复创建上层路径
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

        // 在已创建好的 child unit 上写字段
        DdzDef fake = new DdzDef();
        fake.ddzName = childDef.ddzName;
        fake.rows = childDef.rows;
        fake.unitPathSegments = childDef.unitPathSegments;
        fake.parentDdzName = childDef.parentDdzName;
        writeFieldsOnExistingUnit(cursor, fake, childInstance);

        // 继续递归下一级子 DDZ
        for (Map.Entry<String, DdzDef> entry : defs.entrySet()) {
            DdzDef nextChild = entry.getValue();
            if (!childDef.ddzName.equals(nextChild.parentDdzName)) {
                continue;
            }
            List<Map<String, Object>> nextChildInstances = castInstanceList(childInstance.get(nextChild.ddzName));
            for (Map<String, Object> ins : nextChildInstances) {
                writeDdzChildInstance(cursor, nextChild, ins, defs);
            }
        }
    }

    /**
     * 在“已存在的 unit 节点”上写字段。
     */
    private void writeFieldsOnExistingUnit(Element unit, DdzDef def, Map<String, Object> instance) {
        for (RspCfg row : def.rows) {
            String fieldName = row.getFieldName();
            if (fieldName == null || fieldName.trim().isEmpty()) {
                continue;
            }
            Object value = instance.get(fieldName);
            if (value == null) {
                continue;
            }

            List<String> fullPath = splitPath(row.getXpath());
            if (fullPath.equals(def.unitPathSegments) && def.rows.size() == 1) {
                unit.setText(String.valueOf(value));
                continue;
            }

            List<String> relative = fullPath.subList(def.unitPathSegments.size(), fullPath.size());
            Element cursor = unit;
            for (int i = 0; i < relative.size(); i++) {
                String seg = relative.get(i);
                boolean isLeaf = i == relative.size() - 1;
                if (isLeaf) {
                    Element leaf = cursor.addElement(seg);
                    leaf.setText(String.valueOf(value));
                } else {
                    Element next = cursor.element(seg);
                    if (next == null) {
                        next = cursor.addElement(seg);
                    }
                    cursor = next;
                }
            }
        }
    }

    /**
     * 从 XML 中按 DDZ 定义解析实例列表。
     */
    private List<DdzInstance> parseInstancesFromXml(Element root, DdzDef def) {
        // unitPath 第 0 段是根节点名，查询时从 root 的子层开始
        List<Element> units = selectElements(root, def.unitPathSegments.subList(1, def.unitPathSegments.size()));
        List<DdzInstance> out = new ArrayList<>();

        for (Element unit : units) {
            Map<String, Object> item = new LinkedHashMap<>();
            for (RspCfg row : def.rows) {
                String fieldName = row.getFieldName();
                if (fieldName == null || fieldName.trim().isEmpty()) {
                    continue;
                }

                List<String> full = splitPath(row.getXpath());
                String value;

                // 单字段对象的“节点文本”模式
                if (def.rows.size() == 1 && full.equals(def.unitPathSegments)) {
                    value = unit.getTextTrim();
                } else {
                    // 多字段对象：在 unit 下按相对路径取值
                    List<String> relative = full.subList(def.unitPathSegments.size(), full.size());
                    Element leaf = getByRelativePath(unit, relative);
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

    /**
     * 将 parsed 中的子实例挂接到父实例。
     *
     * 判定规则：若 child.unitElement 在 XML 树上是 parent.unitElement 的后代，
     * 则 child 归属该 parent。
     */
    private void linkParentChild(Map<String, List<DdzInstance>> parsed, Map<String, DdzDef> defs) {
        for (DdzDef child : defs.values()) {
            if (child.parentDdzName == null) {
                continue;
            }
            DdzDef parent = defs.get(child.parentDdzName);
            List<DdzInstance> childInstances = parsed.getOrDefault(child.ddzName, Collections.emptyList());
            List<DdzInstance> parentInstances = parsed.getOrDefault(parent.ddzName, Collections.emptyList());

            for (DdzInstance c : childInstances) {
                DdzInstance holder = null;
                for (DdzInstance p : parentInstances) {
                    if (isAncestor(p.unitElement, c.unitElement)) {
                        holder = p;
                        break;
                    }
                }
                if (holder != null) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> list = (List<Map<String, Object>>) holder.data
                            .computeIfAbsent(child.ddzName, k -> new ArrayList<Map<String, Object>>());
                    list.add(c.data);
                }
            }
        }
    }

    /**
     * 判断 ancestor 是否为 child 的祖先节点（含自身）。
     */
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

    /**
     * 构建 DDZ 定义：
     * - 按 ddzName 分组
     * - 推导 unitPath
     * - 推导父 DDZ（最长前缀匹配）
     */
    private Map<String, DdzDef> buildDefs(List<RspCfg> cfgList) {
        Map<String, List<RspCfg>> byDdz = cfgList.stream()
                .filter(c -> c.getDdzName() != null && !c.getDdzName().trim().isEmpty())
                .collect(Collectors.groupingBy(RspCfg::getDdzName, LinkedHashMap::new, Collectors.toList()));

        Map<String, DdzDef> defs = new LinkedHashMap<>();
        for (Map.Entry<String, List<RspCfg>> e : byDdz.entrySet()) {
            String ddzName = e.getKey();
            List<RspCfg> rows = e.getValue().stream()
                    .sorted(Comparator.comparing(RspCfg::getFieldSeq, Comparator.nullsLast(Integer::compareTo)))
                    .collect(Collectors.toList());

            DdzDef def = new DdzDef();
            def.ddzName = ddzName;
            def.rows = rows;
            def.unitPathSegments = inferUnitPath(rows);
            defs.put(ddzName, def);
        }

        // 父子关系：父路径是子路径前缀，且“最长前缀优先”
        for (DdzDef child : defs.values()) {
            DdzDef bestParent = null;
            int bestLen = -1;
            for (DdzDef parent : defs.values()) {
                if (parent == child) {
                    continue;
                }
                if (isPrefix(parent.unitPathSegments, child.unitPathSegments)
                        && parent.unitPathSegments.size() < child.unitPathSegments.size()
                        && parent.unitPathSegments.size() > bestLen) {
                    bestParent = parent;
                    bestLen = parent.unitPathSegments.size();
                }
            }
            if (bestParent != null) {
                child.parentDdzName = bestParent.ddzName;
                child.parentUnitPathSegments = bestParent.unitPathSegments;
            }
        }

        return defs;
    }

    /**
     * 推导一个 DDZ 对象的 unitPath。
     */
    private List<String> inferUnitPath(List<RspCfg> rows) {
        if (rows.size() == 1) {
            return splitPath(rows.get(0).getXpath());
        }

        // 多字段取 xpath 最长公共前缀
        List<String> lcp = splitPath(rows.get(0).getXpath());
        for (int i = 1; i < rows.size(); i++) {
            lcp = commonPrefix(lcp, splitPath(rows.get(i).getXpath()));
        }
        return lcp;
    }

    /**
     * 计算两个路径的公共前缀。
     */
    private List<String> commonPrefix(List<String> a, List<String> b) {
        int n = Math.min(a.size(), b.size());
        List<String> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (!Objects.equals(a.get(i), b.get(i))) {
                break;
            }
            out.add(a.get(i));
        }
        return out;
    }

    /**
     * 判断 a 是否为 b 前缀。
     */
    private boolean isPrefix(List<String> a, List<String> b) {
        if (a.size() > b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!Objects.equals(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 构建签名原文串：
     * - 按 field_seq 升序
     * - 每项格式 fieldName=value
     * - 多值字段按 XML 出现顺序追加
     * - 使用 | 连接
     */
    private String buildSignPlainText(List<RspCfg> cfgList, Element root) {
        List<RspCfg> sorted = cfgList.stream()
                .sorted(Comparator.comparing(RspCfg::getFieldSeq, Comparator.nullsLast(Integer::compareTo)))
                .collect(Collectors.toList());

        List<String> parts = new ArrayList<>();
        for (RspCfg row : sorted) {
            List<String> full = splitPath(row.getXpath());
            if (full.isEmpty()) {
                continue;
            }
            List<String> relative = full.subList(1, full.size());
            List<Element> leaves = selectElements(root, relative);
            for (Element leaf : leaves) {
                String value = leaf.getTextTrim();
                if (value != null && !value.isEmpty()) {
                    parts.add(row.getFieldName() + "=" + value);
                }
            }
        }
        return String.join("|", parts);
    }

    /**
     * 从 start 节点出发，按相对路径查找所有匹配元素。
     */
    private List<Element> selectElements(Element start, List<String> relativePath) {
        List<Element> current = new ArrayList<>();
        current.add(start);

        for (String seg : relativePath) {
            List<Element> next = new ArrayList<>();
            for (Element e : current) {
                next.addAll(e.elements(seg));
            }
            current = next;
            if (current.isEmpty()) {
                break;
            }
        }
        return current;
    }

    /**
     * 在 start 下按相对路径获取单个元素（每层取第一个同名子节点）。
     */
    private Element getByRelativePath(Element start, List<String> relativePath) {
        Element cur = start;
        for (String seg : relativePath) {
            if (cur == null) {
                return null;
            }
            cur = cur.element(seg);
        }
        return cur;
    }

    /**
     * 按 fullPath 创建对象单元节点。
     * fullPath 第 0 段是根节点，调用方已创建，所以从第 1 段开始处理。
     */
    private Element createUnitElement(Element root, List<String> fullPath) {
        List<String> relative = fullPath.subList(1, fullPath.size());
        Element cursor = root;
        for (int i = 0; i < relative.size(); i++) {
            String seg = relative.get(i);
            boolean isLast = i == relative.size() - 1;
            if (isLast) {
                // unit 节点每个实例都需要新建
                cursor = cursor.addElement(seg);
            } else {
                // 中间容器层可复用
                Element next = cursor.element(seg);
                if (next == null) {
                    next = cursor.addElement(seg);
                }
                cursor = next;
            }
        }
        return cursor;
    }

    /**
     * 将 dtoMap 中某个值安全转为 List<Map<String,Object>>。
     */
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

    /**
     * 从数据库读取 wkeCode 对应配置。
     */
    private List<RspCfg> loadCfg(String wkeCode) {
        List<RspCfg> cfg = rspCfgMapper.selectByWkeCode(wkeCode);
        if (cfg == null || cfg.isEmpty()) {
            throw new IllegalArgumentException("rsp_cfg_t未找到wke_code=" + wkeCode + "的配置");
        }
        return cfg;
    }

    /**
     * 解析 XML 字符串为 dom4j Document。
     */
    private Document parseXml(String xml) {
        try {
            SAXReader reader = new SAXReader();
            return reader.read(new StringReader(xml));
        } catch (Exception e) {
            throw new IllegalArgumentException("XML解析失败: " + e.getMessage(), e);
        }
    }

    /**
     * 将 Document 输出为格式化 XML。
     */
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

    /**
     * 拆分 xpath，过滤空段。
     * 例如 "/Document/GrpHdr/MsgId" -> ["Document","GrpHdr","MsgId"]
     */
    private List<String> splitPath(String xpath) {
        if (xpath == null || xpath.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(xpath.split("/"))
                .filter(s -> s != null && !s.trim().isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * 获取 xpath 第一段（根节点名）。
     */
    private String firstSegment(String xpath) {
        List<String> segs = splitPath(xpath);
        if (segs.isEmpty()) {
            throw new IllegalArgumentException("非法xpath: " + xpath);
        }
        return segs.get(0);
    }

    /**
     * DDZ 定义：一个 ddzName 对应的一组配置与推导结构。
     */
    private static class DdzDef {
        /** DDZ 名称（如 X1/X4） */
        private String ddzName;
        /** 该 DDZ 对应的配置行 */
        private List<RspCfg> rows;
        /** 对象单元路径（用于定位一个实例） */
        private List<String> unitPathSegments;
        /** 父 DDZ 名（根对象为空） */
        private String parentDdzName;
        /** 父对象 unitPath，便于写子对象时求相对路径 */
        private List<String> parentUnitPathSegments;
    }

    /**
     * 解析期实例：
     * - unitElement：该实例在 XML 中对应的节点（用于祖先判断）
     * - data：该实例的字段值（最终进入 dtoMap）
     */
    private static class DdzInstance {
        private final Element unitElement;
        private final Map<String, Object> data;

        private DdzInstance(Element unitElement, Map<String, Object> data) {
            this.unitElement = unitElement;
            this.data = data;
        }
    }
}
