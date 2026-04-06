package com.example.service.message.transform;

import com.example.entity.RspCfg;
import com.example.service.message.transform.TransformDefinitionResolver.StructureAnchor;
import lombok.RequiredArgsConstructor;
import org.dom4j.Element;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class SignPlainTextBuilder {

    public String build(List<RspCfg> cfgList, Element root) {
        List<RspCfg> signRows = cfgList.stream()
                .filter(r -> r.getFieldName() != null && !r.getFieldName().trim().isEmpty())
                .sorted(Comparator.comparing(RspCfg::getFieldSeq, Comparator.nullsLast(Integer::compareTo)))
                .collect(Collectors.toList());

        List<StructureAnchor> structureAnchors = cfgList.stream()
                .filter(c -> c.getDdzName() == null || c.getDdzName().trim().isEmpty())
                .map(c -> new StructureAnchor(XmlPathSupport.splitPath(c.getXpath()), isMultiTag(c.getMultiTag())))
                .filter(a -> !a.getPath().isEmpty())
                .collect(Collectors.toList());

        List<String> parts = new ArrayList<>();
        List<String> rootPath = Collections.singletonList(root.getName());
        appendSignPartsByContext(parts, root, rootPath, signRows, structureAnchors);
        return String.join("|", parts);
    }

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
            List<String> rowPath = XmlPathSupport.splitPath(row.getXpath());
            if (!XmlPathSupport.isPrefix(contextPath, rowPath)) {
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
            List<Element> units = XmlPathSupport.selectElements(context, relAnchorPath);
            for (Element unit : units) {
                appendSignPartsByContext(parts, unit, op.anchorPath, op.groupRows, anchors);
            }
        }
    }

    private void appendRowValuesInContext(List<String> parts, Element context, List<String> contextPath, RspCfg row) {
        List<String> full = XmlPathSupport.splitPath(row.getXpath());
        if (full.isEmpty() || !XmlPathSupport.isPrefix(contextPath, full)) {
            return;
        }
        List<String> relative = full.subList(contextPath.size(), full.size());
        List<Element> leaves = XmlPathSupport.selectElements(context, relative);
        for (Element leaf : leaves) {
            String value = leaf.getTextTrim();
            if (value != null && !value.isEmpty()) {
                parts.add(row.getFieldName() + "=" + value);
            }
        }
    }

    private List<String> findNextMultiAnchor(List<String> leafPath, List<String> contextPath, List<StructureAnchor> anchors) {
        List<String> best = Collections.emptyList();
        int bestLen = Integer.MAX_VALUE;
        for (StructureAnchor anchor : anchors) {
            if (!anchor.isMulti()) {
                continue;
            }
            List<String> path = anchor.getPath();
            if (path.size() <= contextPath.size()) {
                continue;
            }
            if (!XmlPathSupport.isPrefix(contextPath, path)) {
                continue;
            }
            if (!XmlPathSupport.isPrefix(path, leafPath)) {
                continue;
            }
            if (path.size() < bestLen) {
                best = path;
                bestLen = path.size();
            }
        }
        return best;
    }

    private boolean isMultiTag(String multiTag) {
        return multiTag != null && "M".equalsIgnoreCase(multiTag.trim());
    }

    private static class SeqSignOp {
        private final RspCfg singleRow;
        private final List<String> anchorPath;
        private final List<RspCfg> groupRows;

        private SeqSignOp(RspCfg singleRow, List<String> anchorPath, List<RspCfg> groupRows) {
            this.singleRow = singleRow;
            this.anchorPath = anchorPath;
            this.groupRows = groupRows;
        }

        private static SeqSignOp single(RspCfg row) {
            return new SeqSignOp(row, null, null);
        }

        private static SeqSignOp group(List<String> anchorPath, List<RspCfg> rows) {
            return new SeqSignOp(null, anchorPath, rows);
        }

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
}
