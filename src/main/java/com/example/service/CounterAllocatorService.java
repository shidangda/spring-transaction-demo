package com.example.service;

import com.example.entity.Counter;
import com.example.mapper.CounterMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class CounterAllocatorService {

    private final CounterMapper counterMapper;

    @Transactional
    public AllocatedRange allocateRange(String counterName, int step) {
        counterMapper.initIfAbsent(counterName, step);

        while (true) {
            Counter c = counterMapper.selectByName(counterName);
            if (c == null) {
                counterMapper.initIfAbsent(counterName, step);
                continue;
            }

            int expectVersion = c.getVersion();
            int updated = counterMapper.allocateNextRange(counterName, expectVersion, step);
            if (updated > 0) {
                long start = c.getCurrentMax() + 1;
                long end = c.getCurrentMax() + step;
                log.info("分配计数器批次: name={}, range=[{}-{}], version {} -> {}", counterName, start, end, expectVersion, expectVersion + 1);
                return new AllocatedRange(start, end);
            }

            log.warn("计数器批次分配冲突，重试: name={}", counterName);
        }
    }
}


