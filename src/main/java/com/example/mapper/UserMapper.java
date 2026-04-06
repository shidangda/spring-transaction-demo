package com.example.mapper;

import com.example.entity.User;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface UserMapper {
    int insert(User user);

    int updateUsernameById(@Param("id") Long id, @Param("username") String username);

    User selectById(@Param("id") Long id);

    List<User> selectAll();

    User selectByUsername(@Param("username") String username);

    User selectByEmail(@Param("email") String email);

    Integer countByUsername(@Param("username") String username);

    Integer countByEmail(@Param("email") String email);

    int deleteById(@Param("id") Long id);
}
