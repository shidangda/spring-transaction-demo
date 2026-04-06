package com.example.controller;

import com.example.entity.User;
import com.example.service.CounterService;
import com.example.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 用户控制器
 * 提供RESTful API接口
 */
@RestController
@RequestMapping("/users")
@RequiredArgsConstructor
@Slf4j
public class UserController {

    private final UserService userService;
    private final CounterService counterService;

    /**
     * 创建用户
     * POST /api/users
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createUser(@RequestBody User user) {
        Map<String, Object> response = new HashMap<>();

        try {
            log.info("接收到创建用户请求: {}", user.getUsername());
            User createdUser = userService.createUser(user);

            response.put("success", true);
            response.put("message", "用户创建成功");
            response.put("data", createdUser);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("创建用户失败: {}", e.getMessage());

            response.put("success", false);
            response.put("message", "创建用户失败: " + e.getMessage());
            response.put("data", null);

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * 更新用户名
     * PUT /api/users/{id}/username
     */
    @PutMapping("/{id}/username")
    public ResponseEntity<Map<String, Object>> updateUsername(
            @PathVariable Long id,
            @RequestBody Map<String, String> request) {

        Map<String, Object> response = new HashMap<>();
        String newUsername = request.get("username");

        if (newUsername == null || newUsername.trim().isEmpty()) {
            response.put("success", false);
            response.put("message", "用户名不能为空");
            return ResponseEntity.badRequest().body(response);
        }

        try {
            log.info("接收到更新用户名请求: ID={}, 新用户名={}", id, newUsername);
            boolean success = userService.updateUsername(id, newUsername);

            if (success) {
                response.put("success", true);
                response.put("message", "用户名更新成功");

                // 返回更新后的用户信息
                Optional<User> user = userService.findUserById(id);
                response.put("data", user.orElse(null));

                return ResponseEntity.ok(response);
            } else {
                response.put("success", false);
                response.put("message", "用户名更新失败");
                return ResponseEntity.badRequest().body(response);
            }

        } catch (Exception e) {
            log.error("更新用户名失败: {}", e.getMessage());

            response.put("success", false);
            response.put("message", "更新用户名失败: " + e.getMessage());
            response.put("data", null);

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * 在新事务中更新用户名（演示事务传播）
     * PUT /api/users/{id}/username-new-transaction
     */
    @PutMapping("/{id}/username-new-transaction")
    public ResponseEntity<Map<String, Object>> updateUsernameInNewTransaction(
            @PathVariable Long id,
            @RequestBody Map<String, String> request) {

        Map<String, Object> response = new HashMap<>();
        String newUsername = request.get("username");

        if (newUsername == null || newUsername.trim().isEmpty()) {
            response.put("success", false);
            response.put("message", "用户名不能为空");
            return ResponseEntity.badRequest().body(response);
        }

        try {
            log.info("接收到新事务更新用户名请求: ID={}, 新用户名={}", id, newUsername);
            boolean success = userService.updateUsernameInNewTransaction(id, newUsername);

            if (success) {
                response.put("success", true);
                response.put("message", "用户名在新事务中更新成功");

                Optional<User> user = userService.findUserById(id);
                response.put("data", user.orElse(null));

                return ResponseEntity.ok(response);
            } else {
                response.put("success", false);
                response.put("message", "用户名在新事务中更新失败");
                return ResponseEntity.badRequest().body(response);
            }

        } catch (Exception e) {
            log.error("新事务更新用户名失败: {}", e.getMessage());

            response.put("success", false);
            response.put("message", "新事务更新用户名失败: " + e.getMessage());
            response.put("data", null);

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * 批量更新用户名（演示事务传播）
     * PUT /api/users/batch-update-usernames
     */
    @PutMapping("/batch-update-usernames")
    public ResponseEntity<Map<String, Object>> batchUpdateUsernames(
            @RequestBody Map<String, Object> request) {

        Map<String, Object> response = new HashMap<>();

        try {
            @SuppressWarnings("unchecked")
            List<Long> userIds = (List<Long>) request.get("userIds");
            @SuppressWarnings("unchecked")
            List<String> newUsernames = (List<String>) request.get("usernames");

            if (userIds == null || newUsernames == null || userIds.size() != newUsernames.size()) {
                response.put("success", false);
                response.put("message", "用户ID列表和用户名列表不能为空且长度必须相等");
                return ResponseEntity.badRequest().body(response);
            }

            log.info("接收到批量更新用户名请求，用户数量: {}", userIds.size());
            userService.batchUpdateUsernames(userIds, newUsernames);

            response.put("success", true);
            response.put("message", "批量更新用户名成功");
            response.put("data", null);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("批量更新用户名失败: {}", e.getMessage());

            response.put("success", false);
            response.put("message", "批量更新用户名失败: " + e.getMessage());
            response.put("data", null);

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * 获取所有用户
     * GET /api/users
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllUsers() {
        Map<String, Object> response = new HashMap<>();

        try {
            List<User> users = userService.getAllUsers();

            response.put("success", true);
            response.put("message", "查询成功");
            response.put("data", users);
            response.put("count", users.size());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("查询所有用户失败: {}", e.getMessage());

            response.put("success", false);
            response.put("message", "查询失败: " + e.getMessage());
            response.put("data", null);

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * 获取下一个计数器值（演示 Caffeine+MyBatis 批次分配）
     * GET /api/users/counter/{name}/next
     */
    @GetMapping("/counter/{name}/next")
    public ResponseEntity<Map<String, Object>> nextCounter(@PathVariable String name) {
        Map<String, Object> response = new HashMap<>();
        long value = counterService.nextId(name);
        response.put("success", true);
        response.put("message", "获取下一个计数器值成功");
        response.put("data", value);
        return ResponseEntity.ok(response);
    }

    /**
     * 根据ID获取用户
     * GET /api/users/{id}
     */
    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> getUserById(@PathVariable Long id) {
        Map<String, Object> response = new HashMap<>();

        try {
            Optional<User> user = userService.findUserById(id);

            if (user.isPresent()) {
                response.put("success", true);
                response.put("message", "查询成功");
                response.put("data", user.get());
            } else {
                response.put("success", false);
                response.put("message", "用户不存在");
                response.put("data", null);
            }

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("根据ID查询用户失败: {}", e.getMessage());

            response.put("success", false);
            response.put("message", "查询失败: " + e.getMessage());
            response.put("data", null);

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * 根据用户名获取用户
     * GET /api/users/username/{username}
     */
    @GetMapping("/username/{username}")
    public ResponseEntity<Map<String, Object>> getUserByUsername(@PathVariable String username) {
        Map<String, Object> response = new HashMap<>();

        try {
            Optional<User> user = userService.findUserByUsername(username);

            if (user.isPresent()) {
                response.put("success", true);
                response.put("message", "查询成功");
                response.put("data", user.get());
            } else {
                response.put("success", false);
                response.put("message", "用户不存在");
                response.put("data", null);
            }

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("根据用户名查询用户失败: {}", e.getMessage());

            response.put("success", false);
            response.put("message", "查询失败: " + e.getMessage());
            response.put("data", null);

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * 删除用户
     * DELETE /api/users/{id}
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> deleteUser(@PathVariable Long id) {
        Map<String, Object> response = new HashMap<>();

        try {
            boolean success = userService.deleteUser(id);

            if (success) {
                response.put("success", true);
                response.put("message", "用户删除成功");
            } else {
                response.put("success", false);
                response.put("message", "用户删除失败");
            }

            response.put("data", null);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("删除用户失败: {}", e.getMessage());

            response.put("success", false);
            response.put("message", "删除用户失败: " + e.getMessage());
            response.put("data", null);

            return ResponseEntity.badRequest().body(response);
        }
    }
}
