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
            if (cur == null) {
                return null;
            }
            cur = cur.element(seg);
        }
        return cur;
    }
}
