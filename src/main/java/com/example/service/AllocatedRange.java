package com.example.service;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AllocatedRange {
    private final long startInclusive;
    private final long endInclusive;
}


