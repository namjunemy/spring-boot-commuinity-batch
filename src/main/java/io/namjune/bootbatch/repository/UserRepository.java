package io.namjune.bootbatch.repository;

import io.namjune.bootbatch.domain.User;
import io.namjune.bootbatch.domain.enums.UserStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;

public interface UserRepository extends JpaRepository<User, Long> {

    List<User> findByUpdatedDateBeforeAndStatusEquals(LocalDateTime localDateTime, UserStatus status);
}
