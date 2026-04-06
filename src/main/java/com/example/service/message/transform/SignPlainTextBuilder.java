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
import java.util.Set;
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

        Map<String, String> amountCurrencyPathIndex = buildAmountCurrencyPathIndex(signRows);

        List<String> values = new ArrayList<>();
        List<String> rootPath = Collections.singletonList(root.getName());
        appendSignPartsByContext(values, root, rootPath, signRows, structureAnchors, amountCurrencyPathIndex);

        if (values.isEmpty()) {
            return "";
        }
        // 按规范：只拼值，且每个值后都带“|”，最后一个也保留“|”。
        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            sb.append(value).append("|");
        }
        return sb.toString();
    }

    private void appendSignPartsByContext(List<String> values,
                                          Element context,
                                          List<String> contextPath,
                                          List<RspCfg> rows,
                                          List<StructureAnchor> anchors,
                                          Map<String, String> amountCurrencyPathIndex) {
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
                appendRowValuesInContext(values, context, contextPath, op.singleRow, amountCurrencyPathIndex);
                continue;
            }

            List<String> relAnchorPath = op.anchorPath.subList(contextPath.size(), op.anchorPath.size());
            List<Element> units = XmlPathSupport.selectElements(context, relAnchorPath);
            for (Element unit : units) {
                appendSignPartsByContext(values, unit, op.anchorPath, op.groupRows, anchors, amountCurrencyPathIndex);
            }
        }
    }

    private void appendRowValuesInContext(List<String> values,
                                          Element context,
                                          List<String> contextPath,
                                          RspCfg row,
                                          Map<String, String> amountCurrencyPathIndex) {
        List<String> full = XmlPathSupport.splitPath(row.getXpath());
        if (full.isEmpty() || !XmlPathSupport.isPrefix(contextPath, full)) {
            return;
        }
        List<String> relative = full.subList(contextPath.size(), full.size());
        List<String> rawValues = XmlPathSupport.selectValues(context, relative);

        String fullPathKey = joinPath(full);
        String ccyPathKey = amountCurrencyPathIndex.get(fullPathKey);
        if (ccyPathKey != null && !XmlPathSupport.isAttributePath(full)) {
            List<String> ccyPath = XmlPathSupport.splitPath(ccyPathKey);
            if (XmlPathSupport.isPrefix(contextPath, ccyPath)) {
                List<String> ccyRelative = ccyPath.subList(contextPath.size(), ccyPath.size());
                List<String> ccys = XmlPathSupport.selectValues(context, ccyRelative);
                for (int i = 0; i < rawValues.size(); i++) {
                    String amount = rawValues.get(i);
                    if (amount == null || amount.isEmpty()) {
                        continue;
                    }
                    String ccy = i < ccys.size() ? ccys.get(i) : null;
                    if (ccy != null && !ccy.isEmpty()) {
                        values.add(ccy + amount);
                    } else {
                        values.add(amount);
                    }
                }
                return;
            }
        }

        for (String value : rawValues) {
            if (value != null && !value.isEmpty()) {
                values.add(value);
            }
        }
    }

    private Map<String, String> buildAmountCurrencyPathIndex(List<RspCfg> rows) {
        Map<String, String> out = new LinkedHashMap<>();
        Set<String> pathSet = rows.stream()
                .map(RspCfg::getXpath)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        for (RspCfg row : rows) {
            String xpath = row.getXpath();
            if (xpath == null || xpath.trim().isEmpty()) {
                continue;
            }
            List<String> path = XmlPathSupport.splitPath(xpath);
            if (XmlPathSupport.isAttributePath(path)) {
                continue;
            }
            String ccyPath = xpath + "/@Ccy";
            if (pathSet.contains(ccyPath)) {
                out.put(joinPath(path), ccyPath);
            }
        }
        return out;
    }

    private String joinPath(List<String> path) {
        return "/" + String.join("/", path);
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
