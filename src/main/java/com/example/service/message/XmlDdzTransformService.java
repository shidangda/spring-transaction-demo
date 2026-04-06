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
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
            if (!def.multi && !instances.isEmpty()) {
                writeDdzInstance(root, def, instances.get(0), defs);
                continue;
            }
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
        // 按 field_seq 顺序交织写“本对象字段”和“子对象块”，避免顺序错位
        writeDdzUnitByFieldSeq(unit, def, instance, defs);
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

        // 按 field_seq 在 child unit 内继续交织写字段/子对象
        writeDdzUnitByFieldSeq(cursor, childDef, childInstance, defs);
    }

    /**
     * 在同一个 unit 节点内，按 field_seq 顺序写“字段节点 + 子对象节点”。
     *
     * 关键点：
     * 1) 先把“字段写入”与“子对象写入”都抽象为 SeqWriteOp，放进同一个队列；
     * 2) 队列统一按 seq() 升序排序；
     * 3) 再顺序执行，保证 XML 子节点顺序与配置表 field_seq 对齐。
     *
     * 示例（以 CdtTrfTxInf 为例）：
     * - X4 对应最小 field_seq=10（子对象块）
     * - X5.InstrId field_seq=12（字段）
     * - X6.XchgRate field_seq=13（字段）
     * 排序后执行顺序即：X4 -> InstrId -> XchgRate。
     *
     * 递归逻辑：
     * 1、writeDdzUnitByFieldSeq方法在unit单元节点内，按 field_seq 顺序写def的字段节点和子对象节点
     * 2、写字段节点时，写unit单元节点到字段节点的相对路径上的所有节点，此处无递归
     * 3、写子对象节点时，写unit单元节点到子对象的unit单元节点的相对路径上的所有节点；递归构建子对象的单元节点
     *
     */
    private void writeDdzUnitByFieldSeq(Element unit, DdzDef def, Map<String, Object> instance, Map<String, DdzDef> defs) {
        List<SeqWriteOp> ops = new ArrayList<>();

        // 当前对象的“字段操作”入队
        for (RspCfg row : def.rows) {
            if (row.getFieldName() == null || row.getFieldName().trim().isEmpty()) {
                continue;
            }
            ops.add(SeqWriteOp.field(row));
        }

        // 当前对象的“直接子对象操作”入队
        for (DdzDef child : defs.values()) {
            if (!def.ddzName.equals(child.parentDdzName)) {
                continue;
            }
            ops.add(SeqWriteOp.child(child));
        }

        // 统一排序：字段和子对象按同一序号体系比较
        ops.sort(Comparator.comparingInt(SeqWriteOp::seq));

        // 依序执行每个操作
        for (SeqWriteOp op : ops) {
            if (op.fieldRow != null) {
                writeFieldByCfg(unit, def, instance, op.fieldRow);
                continue;
            }

            DdzDef child = op.childDef;
            List<Map<String, Object>> childInstances = castInstanceList(instance.get(child.ddzName));
            if (!child.multi && !childInstances.isEmpty()) {
                writeDdzChildInstance(unit, child, childInstances.get(0), defs);
                continue;
            }
            for (Map<String, Object> childInstance : childInstances) {
                writeDdzChildInstance(unit, child, childInstance, defs);
            }
        }
    }

    /**
     * 按一条字段配置（row）将值写入当前 unit。
     *
     * 规则：
     * - 单字段且字段 xpath 就是 unitPath：直接写 unit 文本；
     * - 其他情况：按相对路径写叶子节点；
     * - multi_tag=M 时可写多个同名叶子。
     */
    private void writeFieldByCfg(Element unit, DdzDef def, Map<String, Object> instance, RspCfg row) {
        String fieldName = row.getFieldName();
        Object value = instance.get(fieldName);
        if (value == null) {
            return;
        }

        List<String> fullPath = splitPath(row.getXpath());
        if (fullPath.equals(def.unitPathSegments) && def.rows.size() == 1) {
            unit.setText(String.valueOf(value));
            return;
        }

        List<String> relative = fullPath.subList(def.unitPathSegments.size(), fullPath.size());
        if (relative.isEmpty()) {
            return;
        }

        boolean isMulti = isMultiTag(row.getMultiTag());
        writeLeafByRelativePath(unit, relative, value, isMulti);
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
     * - 推导 unitPath（支持“结构行”参与推导）
     * - 推导父 DDZ（最长前缀匹配）
     */
    private Map<String, DdzDef> buildDefs(List<RspCfg> cfgList) {
        Map<String, List<RspCfg>> byDdz = cfgList.stream()
                .filter(c -> c.getDdzName() != null && !c.getDdzName().trim().isEmpty())
                .collect(Collectors.groupingBy(RspCfg::getDdzName, LinkedHashMap::new, Collectors.toList()));

        // 结构行：ddz_name 为空的配置行，作为“对象层级锚点”参与 unitPath 推导。
        List<StructureAnchor> structureAnchors = cfgList.stream()
                .filter(c -> c.getDdzName() == null || c.getDdzName().trim().isEmpty())
                .map(c -> new StructureAnchor(splitPath(c.getXpath()), isMultiTag(c.getMultiTag())))
                .filter(a -> !a.path.isEmpty())
                .collect(Collectors.toList());

        Map<String, DdzDef> defs = new LinkedHashMap<>();
        for (Map.Entry<String, List<RspCfg>> e : byDdz.entrySet()) {
            String ddzName = e.getKey();
            List<RspCfg> rows = e.getValue().stream()
                    .sorted(Comparator.comparing(RspCfg::getFieldSeq, Comparator.nullsLast(Integer::compareTo)))
                    .collect(Collectors.toList());

            DdzDef def = new DdzDef();
            def.ddzName = ddzName;
            def.rows = rows;
            def.declaredChildren = resolveDeclaredChildDdzNames(ddzName);

            // 对“容器型 DDZ（实体类中声明了子 DDZ 列表）”优先用结构锚点回退，
            // 例如 X6 只有 XchgRate 一条字段时，也能把 unitPath 锚到 /.../CdtTrfTxInf。
            boolean preferStructureAnchor = !def.declaredChildren.isEmpty();
            def.unitPathSegments = inferUnitPath(rows, structureAnchors, preferStructureAnchor);
            // 优先用结构锚点判定对象是否可重复，避免被字段行 multi_tag 误导。
            def.multi = resolveDefMulti(def, structureAnchors);
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
     *
     * 参数说明：
     * - rows: 当前 ddz_name 的字段配置行；
     * - structureAnchors: 来自结构行的锚点集合；
     * - preferStructureAnchor: 是否优先回退到结构锚点（容器型 DDZ 通常为 true）。
     *
     * 规则：
     * 1) 先求 base 路径：
     *    - 单字段：base = 字段 xpath
     *    - 多字段：base = 多字段 xpath 的最长公共前缀
     * 2) 若 preferStructureAnchor=false，直接返回 base；
     * 3) 若 preferStructureAnchor=true，从结构锚点里找“最贴近 base 的祖先路径”作为 unitPath。
     */
    private List<String> inferUnitPath(List<RspCfg> rows, List<StructureAnchor> structureAnchors, boolean preferStructureAnchor) {
        List<String> base;
        if (rows.size() == 1) {
            base = splitPath(rows.get(0).getXpath());
        } else {
            // 多字段取 xpath 最长公共前缀
            List<String> lcp = splitPath(rows.get(0).getXpath());
            for (int i = 1; i < rows.size(); i++) {
                lcp = commonPrefix(lcp, splitPath(rows.get(i).getXpath()));
            }
            base = lcp;
        }

        if (!preferStructureAnchor) {
            return base;
        }

        List<String> anchor = findBestStructureAnchor(base, structureAnchors);
        return anchor.isEmpty() ? base : anchor;
    }

    /**
     * 从结构锚点中挑选“最贴近 base 的祖先路径”（最长前缀）。
     *
     * 例：
     * - base = /.../CdtTrfTxInf/XchgRate
     * - 结构锚点候选：/.../CdtTrfTxInf, /.../CdtTrfTxInf/PmtId
     *   其中只有 /.../CdtTrfTxInf 是 base 的祖先前缀，最终返回它。
     */
    private List<String> findBestStructureAnchor(List<String> base, List<StructureAnchor> structureAnchors) {
        List<String> best = Collections.emptyList();
        int bestLen = -1;
        for (StructureAnchor anchor : structureAnchors) {
            List<String> sp = anchor.path;
            if (sp.size() < base.size() && isPrefix(sp, base) && sp.size() > bestLen) {
                best = sp;
                bestLen = sp.size();
            }
        }
        return best;
    }

    /**
     * 判定一个 DDZ 是否“对象可重复”（def.multi）。
     *
     * 优先级：
     * 1) 若 unitPath 与某个结构锚点路径完全一致，则直接使用锚点的 multi；
     * 2) 否则回退到字段行的 multi_tag（任一字段为 M 则视为多）。
     *
     * 这样可以避免：仅因字段行是 N，就把本应多包的对象误判为单包。
     */
    private boolean resolveDefMulti(DdzDef def, List<StructureAnchor> structureAnchors) {
        StructureAnchor exact = findStructureAnchorByPath(def.unitPathSegments, structureAnchors);
        if (exact != null) {
            return exact.multi;
        }
        return def.rows.stream().anyMatch(r -> isMultiTag(r.getMultiTag()));
    }

    /**
     * 在结构锚点集合中按“路径完全相等”查找锚点。
     */
    private StructureAnchor findStructureAnchorByPath(List<String> path, List<StructureAnchor> structureAnchors) {
        for (StructureAnchor anchor : structureAnchors) {
            if (Objects.equals(anchor.path, path)) {
                return anchor;
            }
        }
        return null;
    }

    /**
     * 通过 DDZ 实体类反射出其声明的子 DDZ 名称（List<X4>/List<X5> -> X4/X5）。
     */
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
            // 非关键路径：解析失败时视为“未声明子 DDZ”。
        }
        return out;
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
     * 构建签名原文串。
     *
     * 与 writeDdzUnitByFieldSeq 的设计保持一致：
     * - 都采用“统一操作队列 + seq 排序”思想；
     * - 写 XML 时用 SeqWriteOp；签名拼接时用 SeqSignOp；
     * - 目标都是让顺序严格贴合 field_seq 与结构层级。
     *
     * 关键规则：
     * 1) 先把可签名字段（field_name 非空）按 field_seq 排序；
     * 2) 递归按“最近下一层 multi 锚点”分组（例如 CdtTrfTxInf / PmtId）；
     * 3) 每一层内把“单字段操作 + 分组操作”统一为 SeqSignOp 再排序；
     * 4) 组内按父实例出现顺序处理，确保 EndToEndId/TxId 这类字段按“同包组合”拼接。
     */
    private String buildSignPlainText(List<RspCfg> cfgList, Element root) {
        List<RspCfg> signRows = cfgList.stream()
                .filter(r -> r.getFieldName() != null && !r.getFieldName().trim().isEmpty())
                .sorted(Comparator.comparing(RspCfg::getFieldSeq, Comparator.nullsLast(Integer::compareTo)))
                .collect(Collectors.toList());

        List<StructureAnchor> structureAnchors = cfgList.stream()
                .filter(c -> c.getDdzName() == null || c.getDdzName().trim().isEmpty())
                .map(c -> new StructureAnchor(splitPath(c.getXpath()), isMultiTag(c.getMultiTag())))
                .filter(a -> !a.path.isEmpty())
                .collect(Collectors.toList());

        List<String> parts = new ArrayList<>();
        List<String> rootPath = Collections.singletonList(root.getName());
        appendSignPartsByContext(parts, root, rootPath, signRows, structureAnchors);
        return String.join("|", parts);
    }

    /**
     * 在某个上下文节点（context）内递归拼签名。
     *
     * 这段是签名串的“主调度器”，相当于写 XML 时的 writeDdzUnitByFieldSeq：
     * - 先把 rows 拆成两类：
     *   a) standalone：在当前层可直接取值的字段；
     *   b) grouped：需要进入下一层 multi 锚点后再处理的字段组。
     * - 再把两类都包装成 SeqSignOp，统一按 seq() 排序后执行。
     *
     * 这样可以保证：
     * - 同层字段与分组块顺序可比较；
     * - 分组块内部按“父实例顺序”递归，保持同包字段组合关系。
     */
    private void appendSignPartsByContext(List<String> parts,
                                          Element context,
                                          List<String> contextPath,
                                          List<RspCfg> rows,
                                          List<StructureAnchor> anchors) {
        if (rows == null || rows.isEmpty()) {
            return;
        }

        Map<List<String>, List<RspCfg>> grouped = new LinkedHashMap<>();
        List<RspCfg> standalone = new ArrayList<>();

        for (RspCfg row : rows) {
            List<String> rowPath = splitPath(row.getXpath());
            if (!isPrefix(contextPath, rowPath)) {
                continue;
            }
            List<String> nextAnchor = findNextMultiAnchor(rowPath, contextPath, anchors);
            if (nextAnchor.isEmpty()) {
                standalone.add(row);
            } else {
                grouped.computeIfAbsent(nextAnchor, k -> new ArrayList<>()).add(row);
            }
        }

        List<SeqSignOp> ops = new ArrayList<>();
        for (RspCfg row : standalone) {
            ops.add(SeqSignOp.single(row));
        }
        for (Map.Entry<List<String>, List<RspCfg>> e : grouped.entrySet()) {
            ops.add(SeqSignOp.group(e.getKey(), e.getValue()));
        }
        ops.sort(Comparator.comparingInt(SeqSignOp::seq));

        for (SeqSignOp op : ops) {
            if (op.singleRow != null) {
                appendRowValuesInContext(parts, context, contextPath, op.singleRow);
                continue;
            }

            List<String> relAnchorPath = op.anchorPath.subList(contextPath.size(), op.anchorPath.size());
            List<Element> units = selectElements(context, relAnchorPath);
            for (Element unit : units) {
                appendSignPartsByContext(parts, unit, op.anchorPath, op.groupRows, anchors);
            }
        }
    }

    /**
     * 在当前 context 内追加某个字段 row 的签名项。
     *
     * 注意：这里限定在 contextPath 作用域内取值，避免跨包“串值”。
     */
    private void appendRowValuesInContext(List<String> parts, Element context, List<String> contextPath, RspCfg row) {
        List<String> full = splitPath(row.getXpath());
        if (full.isEmpty() || !isPrefix(contextPath, full)) {
            return;
        }
        List<String> relative = full.subList(contextPath.size(), full.size());
        List<Element> leaves = selectElements(context, relative);
        for (Element leaf : leaves) {
            String value = leaf.getTextTrim();
            if (value != null && !value.isEmpty()) {
                parts.add(row.getFieldName() + "=" + value);
            }
        }
    }

    /**
     * 找到 leafPath 在当前 context 下“最近的下一层 multi 锚点”。
     *
     * 条件：
     * - 锚点必须是 multi；
     * - 锚点必须在 contextPath 之下；
     * - 锚点必须是 leafPath 的前缀；
     * - 多个候选时取“最浅但比 context 深一层及以上”的最近锚点（路径最短）。
     *
     * 目的：逐层下钻分组，而不是一次跳到底层，
     * 从而保持签名拼接顺序与 XML 结构层级一致。
     */
    private List<String> findNextMultiAnchor(List<String> leafPath, List<String> contextPath, List<StructureAnchor> anchors) {
        List<String> best = Collections.emptyList();
        int bestLen = Integer.MAX_VALUE;
        for (StructureAnchor anchor : anchors) {
            if (!anchor.multi) {
                continue;
            }
            List<String> path = anchor.path;
            if (path.size() <= contextPath.size()) {
                continue;
            }
            if (!isPrefix(contextPath, path)) {
                continue;
            }
            if (!isPrefix(path, leafPath)) {
                continue;
            }
            if (path.size() < bestLen) {
                best = path;
                bestLen = path.size();
            }
        }
        return best;
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
     * 结构锚点（StructureAnchor）：
     * 对应 rsp_cfg_t 中 ddz_name 为空的“结构行”。
     *
     * 为什么要有这个类：
     * - 字段行（ddz_name 非空）描述“叶子值映射”；
     * - 结构行（ddz_name 为空）描述“层级骨架与是否可重复”。
     *
     * 这两个信息都必须用到：
     * 1) unitPath/父子推导：需要结构骨架避免把容器对象锚到叶子；
     * 2) 对象多值判定：应优先看结构节点是否 multi；
     * 3) 签名分组：需要按 multi 锚点逐层分包，避免跨包串值。
     */
    private static class StructureAnchor {
        /** 结构节点路径（已 split），例如 [FIToFICstmrCdtTrf, CdtTrfTxInf, PmtId] */
        private final List<String> path;
        /** 该结构节点是否可重复（multi_tag=M） */
        private final boolean multi;

        private StructureAnchor(List<String> path, boolean multi) {
            this.path = path;
            this.multi = multi;
        }
    }

    /**
     * XML 写入阶段的“顺序操作”抽象：
     * - fieldRow != null：表示“写一个字段”；
     * - childDef != null：表示“写一个子对象块”。
     *
     * 设计目的：
     * 让字段与子对象可以放在同一列表里统一排序，
     * 从而按 field_seq 精确控制最终 XML 子节点顺序。
     */
    private static class SeqWriteOp {
        /** 字段操作载荷（二选一） */
        private final RspCfg fieldRow;
        /** 子对象操作载荷（二选一） */
        private final DdzDef childDef;

        private SeqWriteOp(RspCfg fieldRow, DdzDef childDef) {
            this.fieldRow = fieldRow;
            this.childDef = childDef;
        }

        /** 工厂方法：构造“字段写入”操作 */
        private static SeqWriteOp field(RspCfg row) {
            return new SeqWriteOp(row, null);
        }

        /** 工厂方法：构造“子对象写入”操作 */
        private static SeqWriteOp child(DdzDef def) {
            return new SeqWriteOp(null, def);
        }

        /**
         * 返回当前操作的排序序号。
         *
         * - 字段操作：直接使用该字段的 field_seq；
         * - 子对象操作：使用该子对象“所有字段中最小 field_seq”。
         *
         * 示例：
         * 子对象 X4 有字段：
         *   EndToEndId(field_seq=10), TxId(field_seq=11)
         * 则 SeqWriteOp.child(X4).seq() = 10。
         * 这意味着整个 X4 子对象块会排在 field_seq=12 的 InstrId 之前。
         */
        private int seq() {
            if (fieldRow != null) {
                return fieldRow.getFieldSeq() == null ? Integer.MAX_VALUE : fieldRow.getFieldSeq();
            }
            if (childDef == null || childDef.rows == null || childDef.rows.isEmpty()) {
                return Integer.MAX_VALUE;
            }
            Integer min = childDef.rows.stream()
                    .map(RspCfg::getFieldSeq)
                    .filter(Objects::nonNull)
                    .min(Integer::compareTo)
                    .orElse(Integer.MAX_VALUE);
            return min;
        }
    }

    /**
     * 签名拼接阶段的“顺序操作”抽象（与 SeqWriteOp 同风格）。
     *
     * - singleRow != null：表示“直接拼一个字段”；
     * - anchorPath/groupRows != null：表示“进入一个分组锚点继续递归拼接”。
     *
     * 设计目的：
     * 把“单字段拼接”和“分组递归拼接”放在同一队列里排序，
     * 让签名串顺序与写 XML 顺序共享同一套 seq 语义。
     */
    private static class SeqSignOp {
        /** 单字段操作载荷（二选一） */
        private final RspCfg singleRow;
        /** 分组锚点路径（二选一） */
        private final List<String> anchorPath;
        /** 该分组下待处理字段 */
        private final List<RspCfg> groupRows;

        private SeqSignOp(RspCfg singleRow, List<String> anchorPath, List<RspCfg> groupRows) {
            this.singleRow = singleRow;
            this.anchorPath = anchorPath;
            this.groupRows = groupRows;
        }

        /** 工厂方法：构造“单字段拼接”操作 */
        private static SeqSignOp single(RspCfg row) {
            return new SeqSignOp(row, null, null);
        }

        /** 工厂方法：构造“分组递归拼接”操作 */
        private static SeqSignOp group(List<String> anchorPath, List<RspCfg> rows) {
            return new SeqSignOp(null, anchorPath, rows);
        }

        /**
         * 返回当前操作的排序序号：
         * - 单字段：取字段自身 field_seq；
         * - 分组：取分组内最小 field_seq（等价于该分组在当前层的起始位置）。
         */
        private int seq() {
            if (singleRow != null) {
                return singleRow.getFieldSeq() == null ? Integer.MAX_VALUE : singleRow.getFieldSeq();
            }
            if (groupRows == null || groupRows.isEmpty()) {
                return Integer.MAX_VALUE;
            }
            return groupRows.stream()
                    .map(RspCfg::getFieldSeq)
                    .filter(Objects::nonNull)
                    .min(Integer::compareTo)
                    .orElse(Integer.MAX_VALUE);
        }
    }

    /**
     * DDZ 定义：接口对象配置定义，一个 ddzName 对应的一组配置与推导结构。
     * 概念解释：
     * unitPath：表示“一个 DDZ接口实例在 XML 里对应的单位节点路径”。可以理解成，这个 DDZ接口实例在 XML 中‘挂在哪个节点上’
     */
    private static class DdzDef {
        /** DDZ 名称（如 X1/X4） */
        private String ddzName;
        /** 该 DDZ 对应的配置行，DDZ接口可能有多个字段配置行 */
        private List<RspCfg> rows;
        /** 该 DDZ 是否多值（注意DDZ接口是否是多包，是通过结构锚点判定的，与配置表记录的多包表示区分） */
        private boolean multi;
        /** 对象单元路径（用于定位一个实例） */
        private List<String> unitPathSegments;
        /** 在实体类中声明的子 DDZ（例如 X6 声明了 X4/X5） */
        private Set<String> declaredChildren;
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
