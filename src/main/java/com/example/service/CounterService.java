package com.example.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 计数器自增服务（本地批次缓存 + 数据库乐观锁）。
 *
 * <p><b>设计要点</b></p>
 * <ul>
 *   <li><b>事务生效</b>：通过 {@code CounterAllocatorService#allocateRange} 的 {@code @Transactional}
 *       在单独的服务中对数据库区间进行分配，由本服务间接调用，避免同类自调用导致事务失效。</li>
 *   <li><b>并发控制</b>：针对每个计数器名使用 {@link ReentrantLock} 做细粒度加锁，配合双重检查，
 *       仅为避免“本机实例上的并发回源”与重复分配；不同计数器互不干扰。</li>
 *   <li><b>一致性</b>：全局唯一性依赖数据库端的“乐观锁 + CAS 更新”推进 {@code current_max}，
 *       多实例并发分配互斥，区间不重叠。</li>
 * </ul>
 *
 * <p><b>为什么在申请数据库批次前加细粒度锁？</b></p>
 * <ul>
 *   <li><b>目的</b>：防止同一实例内同一计数器并发回源，避免多余批次、噪声日志与瞬时写放大。</li>
 *   <li><b>方式</b>：按 key 分段加锁（{@code keyLocks}），进入锁后<strong>二次检查</strong>缓存以拦截边界竞争。</li>
 * </ul>
 *
 * <p><b>为什么使用 ReentrantLock 而非 synchronized？</b></p>
 * <ul>
 *   <li><b>可控性</b>：显式加解锁、支持 {@code tryLock}、可选公平锁，便于后续扩展（如超时）。</li>
 *   <li><b>可读性</b>：{@code ConcurrentHashMap<String, ReentrantLock>} 自然表达“一 key 一锁”。</li>
 *   <li><b>可观测性</b>：线程 dump 更清晰，避免不可中断等待与锁粗化风险。</li>
 * </ul>
 *
 * <p><b>为什么数据库采用乐观锁而非悲观锁？</b></p>
 * <ul>
 *   <li><b>伸缩性</b>：悲观锁会集中阻塞；乐观锁仅在更新冲突时轻量重试。</li>
 *   <li><b>低开销</b>：无需长事务持锁，降低死锁与 MVCC 压力。</li>
 *   <li><b>多实例友好</b>：并发争用，整体延迟更小。</li>
 *   <li><b>业务契合</b>：计数器单调递增、幂等可重试，天然适合版本校验。</li>
 * </ul>
 *
 * <p><b>风险与工程对策</b></p>
 * <ul>
 *   <li>高冲突导致重试放大与尾延迟上升：需退避/限流与监控。</li>
 *   <li>丢失更新来源于未全链路版本校验或读写分离读到旧版本：统一写路径与强一致读。</li>
 *   <li>复杂约束下的写偏差：配合唯一/检查约束与幂等策略。</li>
 *   <li>兜底手段：唯一约束、防重放幂等键、Outbox/事件溯源、监控冲突率与降级开关。</li>
 * </ul>
 *
 * <p><b>一句话</b>：本地“每计数器 ReentrantLock + 双重检查”保证实例内不重复回源；
 * 全局一致性依赖“乐观锁版本号 + CAS 更新”，在多实例高并发下更稳更快。</p>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CounterService {

    private static final int BATCH_SIZE = 50;

    private final CounterAllocatorService allocatorService;

    private Cache<String, RangeHolder> cache;

    private final ConcurrentHashMap<String, ReentrantLock> keyLocks = new ConcurrentHashMap<>();

    @PostConstruct
    /**
     * 初始化本地批次缓存。
     * <p>使用 Caffeine 设置写入后过期与最大容量，避免长时间持有无用批次。</p>
     */
    public void init() {
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(3))
                .maximumSize(1000)
                .build();
    }

    /**
     * 获取指定计数器的下一个唯一 ID。
     *
     * <p>流程：先尝试从本地缓存批次中获取；若用尽，则在“该计数器”级别加锁并双重检查，
     * 缓存仍无可用则通过 {@link #allocatorService} 从数据库按批分配区间并回填缓存。</p>
     *
     * @param counterName 计数器名称，不能为空
     * @return 下一个唯一自增 ID
     * @throws NullPointerException 当 {@code counterName} 为空时
     */
    public long nextId(String counterName) {
        Objects.requireNonNull(counterName, "counterName");

        RangeHolder holder = cache.getIfPresent(counterName);
        if (holder != null) {
            Long id = holder.tryNext();
            if (id != null) {
                return id;
            }
        }

        ReentrantLock lock = keyLocks.computeIfAbsent(counterName, k -> new ReentrantLock());
        lock.lock();
        try {
            // 双重检查，避免重复加载
            holder = cache.getIfPresent(counterName);
            if (holder != null) {
                Long id = holder.tryNext();
                if (id != null) {
                    return id;
                }
            }

            // 从数据库申请新批次
            AllocatedRange range = allocatorService.allocateRange(counterName, BATCH_SIZE);
            RangeHolder newHolder = new RangeHolder(range.getStartInclusive(), range.getEndInclusive());
            cache.put(counterName, newHolder);
            return newHolder.tryNext();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 批次持有者：以闭区间 [startInclusive, endInclusive] 形式保存可用 ID 段。
     */
    private static class RangeHolder {
        private long next;
        private final long maxInclusive;

        RangeHolder(long startInclusive, long endInclusive) {
            this.next = startInclusive;
            this.maxInclusive = endInclusive;
        }

        /**
         * 线程安全地获取下一个 ID；当批次用尽时返回 {@code null}。
         */
        synchronized Long tryNext() {
            if (next <= maxInclusive) {
                return next++;
            }
            return null;
        }
    }
}
