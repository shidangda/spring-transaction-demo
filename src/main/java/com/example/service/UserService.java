package com.example.service;

import com.example.entity.User;
import com.example.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * 用户服务层
 * 演示Spring事务管理的各种用法
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {

    private final UserMapper userMapper;

    /**
     * 创建用户 - 使用默认事务配置
     * 
     * @Transactional 默认配置：
     *                - propagation: REQUIRED (如果存在事务则加入，否则创建新事务)
     *                - isolation: DEFAULT (使用数据库默认隔离级别)
     *                - rollbackFor: RuntimeException.class (运行时异常回滚)
     */
    @Transactional
    public User createUser(User user) {
        log.info("开始创建用户: {}", user.getUsername());

        // 检查用户名是否已存在
        if (userMapper.countByUsername(user.getUsername()) > 0) {
            throw new RuntimeException("用户名已存在: " + user.getUsername());
        }

        // 检查邮箱是否已存在
        if (userMapper.countByEmail(user.getEmail()) > 0) {
            throw new RuntimeException("邮箱已存在: " + user.getEmail());
        }

        user.setCreatedTime(LocalDateTime.now());
        user.setUpdatedTime(LocalDateTime.now());
        userMapper.insert(user);
        User savedUser = userMapper.selectById(user.getId());
        log.info("用户创建成功: {}", savedUser.getUsername());

        return savedUser;
    }

    /**
     * 更新用户名 - 演示事务回滚
     * 故意抛出异常来演示事务回滚机制
     */
    @Transactional(rollbackFor = Exception.class)
    public boolean updateUsername(Long userId, String newUsername) {
        log.info("开始更新用户ID: {} 的用户名为: {}", userId, newUsername);

        // 检查新用户名是否已存在
        if (userMapper.countByUsername(newUsername) > 0) {
            throw new RuntimeException("用户名已存在: " + newUsername);
        }

        // 查找用户
        User found = userMapper.selectById(userId);
        if (found == null) {
            throw new RuntimeException("用户不存在，ID: " + userId);
        }

        User user = found;
        String oldUsername = user.getUsername();

        // 更新用户名
        int updatedRows = userMapper.updateUsernameById(userId, newUsername);

        if (updatedRows > 0) {
            log.info("用户名更新成功: {} -> {}", oldUsername, newUsername);

            // 模拟业务逻辑：如果新用户名包含"test"，则回滚事务
            if (newUsername.contains("test")) {
                log.warn("检测到测试用户名，触发事务回滚");
                throw new RuntimeException("测试用户名不允许保存，事务将回滚");
            }

            return true;
        } else {
            log.warn("用户名更新失败，未找到用户ID: {}", userId);
            return false;
        }
    }

    /**
     * 更新用户名 - 演示新事务传播
     * 使用REQUIRES_NEW，即使外层事务回滚，这个操作也会提交
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean updateUsernameInNewTransaction(Long userId, String newUsername) {
        log.info("在新事务中更新用户名: ID={}, 新用户名={}", userId, newUsername);

        try {
            return updateUsername(userId, newUsername);
        } catch (Exception e) {
            log.error("新事务中更新用户名失败: {}", e.getMessage());
            // 在新事务中，即使这里抛出异常，也不会影响外层事务
            return false;
        }
    }

    /**
     * 批量更新用户名 - 演示事务传播
     * 外层事务，内部调用其他事务方法
     */
    @Transactional
    public void batchUpdateUsernames(List<Long> userIds, List<String> newUsernames) {
        log.info("开始批量更新用户名，用户数量: {}", userIds.size());

        for (int i = 0; i < userIds.size(); i++) {
            try {
                Long userId = userIds.get(i);
                String newUsername = newUsernames.get(i);

                // 这里会加入当前事务
                updateUsername(userId, newUsername);

            } catch (Exception e) {
                log.error("批量更新中第{}个用户失败: {}", i + 1, e.getMessage());
                // 如果任何一个更新失败，整个批量操作都会回滚
                throw e;
            }
        }

        log.info("批量更新用户名完成");
    }

    /**
     * 只读事务 - 优化查询性能
     */
    @Transactional(readOnly = true)
    public List<User> getAllUsers() {
        log.info("查询所有用户（只读事务）");
        return userMapper.selectAll();
    }

    /**
     * 根据ID查找用户 - 只读事务
     */
    @Transactional(readOnly = true)
    public Optional<User> findUserById(Long id) {
        log.info("根据ID查找用户: {}", id);
        return Optional.ofNullable(userMapper.selectById(id));
    }

    /**
     * 根据用户名查找用户 - 只读事务
     */
    @Transactional(readOnly = true)
    public Optional<User> findUserByUsername(String username) {
        log.info("根据用户名查找用户: {}", username);
        return Optional.ofNullable(userMapper.selectByUsername(username));
    }

    /**
     * 删除用户 - 演示事务回滚
     */
    @Transactional
    public boolean deleteUser(Long userId) {
        log.info("开始删除用户: {}", userId);

        if (userMapper.selectById(userId) == null) {
            throw new RuntimeException("用户不存在，ID: " + userId);
        }

        userMapper.deleteById(userId);
        log.info("用户删除成功: {}", userId);

        return true;
    }
}
