package com.example.service.message.transform;

import org.dom4j.Element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

final class XmlPathSupport {

    private XmlPathSupport() {
    }

    static List<String> splitPath(String xpath) {
        if (xpath == null || xpath.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(xpath.split("/"))
                .filter(s -> s != null && !s.trim().isEmpty())
                .collect(Collectors.toList());
    }

    static List<String> commonPrefix(List<String> a, List<String> b) {
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

    static boolean isPrefix(List<String> a, List<String> b) {
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

    static List<Element> selectElements(Element start, List<String> relativePath) {
        List<Element> current = new ArrayList<>();
        current.add(start);

        for (String seg : relativePath) {
            if (isAttributeSegment(seg)) {
                return Collections.emptyList();
            }
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

    static Element getByRelativePath(Element start, List<String> relativePath) {
        Element cur = start;
        for (String seg : relativePath) {
            if (isAttributeSegment(seg)) {
                return cur;
            }
            if (cur == null) {
                return null;
            }
            cur = cur.element(seg);
        }
        return cur;
    }

    static boolean isAttributeSegment(String seg) {
        return seg != null && seg.startsWith("@");
    }

    static boolean isAttributePath(List<String> path) {
        return path != null && !path.isEmpty() && isAttributeSegment(path.get(path.size() - 1));
    }

    static String attributeName(String seg) {
        if (seg == null) {
            return null;
        }
        return seg.startsWith("@") ? seg.substring(1) : seg;
    }

    static String getSingleValue(Element start, List<String> relativePath) {
        List<String> values = selectValues(start, relativePath);
        return values.isEmpty() ? null : values.get(0);
    }

    static List<String> selectValues(Element start, List<String> relativePath) {
        if (start == null) {
            return Collections.emptyList();
        }
        if (relativePath == null || relativePath.isEmpty()) {
            String text = start.getTextTrim();
            return text == null || text.isEmpty() ? Collections.emptyList() : Collections.singletonList(text);
        }

        List<Element> current = new ArrayList<>();
        current.add(start);

        for (int i = 0; i < relativePath.size(); i++) {
            String seg = relativePath.get(i);
            boolean isLast = i == relativePath.size() - 1;

            // 属性路径：.../@Attr
            if (isAttributeSegment(seg)) {
                if (!isLast) {
                    return Collections.emptyList();
                }
                String attrName = attributeName(seg);
                List<String> out = new ArrayList<>();
                for (Element e : current) {
                    String v = e.attributeValue(attrName);
                    if (v != null && !v.isEmpty()) {
                        out.add(v);
                    }
                }
                return out;
            }

            // 特殊路径：.../SttlmPrty-/A03[/XX]
            // 语义：从 <SttlmPrty> 的文本 "/A03/12345" 中提取 12345。
            if (isTaggedContainerSegment(seg)) {
                if (i + 1 >= relativePath.size()) {
                    return Collections.emptyList();
                }
                String tagCode = relativePath.get(i + 1);
                List<Element> taggedNodes = new ArrayList<>();
                String realNodeName = taggedElementName(seg);
                for (Element e : current) {
                    taggedNodes.addAll(e.elements(realNodeName));
                }
                if (taggedNodes.isEmpty()) {
                    return Collections.emptyList();
                }

                List<String> out = new ArrayList<>();
                for (Element e : taggedNodes) {
                    String extracted = extractTaggedValue(e.getTextTrim(), tagCode);
                    if (extracted != null && !extracted.isEmpty()) {
                        out.add(extracted);
                    }
                }
                return out;
            }

            List<Element> next = new ArrayList<>();
            for (Element e : current) {
                next.addAll(e.elements(seg));
            }
            current = next;
            if (current.isEmpty()) {
                return Collections.emptyList();
            }

            if (isLast) {
                List<String> out = new ArrayList<>();
                for (Element e : current) {
                    String v = e.getTextTrim();
                    if (v != null && !v.isEmpty()) {
                        out.add(v);
                    }
                }
                return out;
            }
        }

        return Collections.emptyList();
    }

    static boolean isTaggedContainerSegment(String seg) {
        return seg != null && seg.endsWith("-") && seg.length() > 1;
    }

    static String taggedElementName(String seg) {
        return isTaggedContainerSegment(seg) ? seg.substring(0, seg.length() - 1) : seg;
    }

    static String extractTaggedValue(String text, String tagCode) {
        if (text == null || tagCode == null || tagCode.trim().isEmpty()) {
            return null;
        }
        String t = text.trim();
        String prefix = "/" + tagCode.trim() + "/";
        if (!t.startsWith(prefix)) {
            return null;
        }
        String v = t.substring(prefix.length()).trim();
        return v.isEmpty() ? null : v;
    }
}
