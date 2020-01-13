package io.namjune.bootbatch.jobs;

import io.namjune.bootbatch.domain.User;
import io.namjune.bootbatch.domain.enums.UserStatus;
import io.namjune.bootbatch.repository.UserRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@Configuration
public class InactiveUserConfig {

    private static final int CHUNK_SIZE = 15;
    private final EntityManagerFactory entityManagerFactory;
    private final UserRepository userRepository;

    @Bean
    public Job inactiveUserJob(JobBuilderFactory jobBuilderFactory, Step inactiveJobStep) {
        return jobBuilderFactory.get("inactiveUserJob")
            .preventRestart()
            .start(inactiveJobStep)
            .build();
    }

    @Bean
    public Step inactiveJobStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("inactiveUserStep")
            .<User, User>chunk(CHUNK_SIZE)
            .reader(inactiveUserJpaReader())
            .processor(inactiveUserProcessor())
            .writer(inactiveUserWriter())
            .build();
    }

    @Bean(destroyMethod = "")
    @StepScope
    public JpaPagingItemReader<User> inactiveUserJpaReader() {
        JpaPagingItemReader<User> jpaPagingItemReader = new JpaPagingItemReader() {

            @Override
            public int getPage() {
                return 0;
            }
        };

        jpaPagingItemReader.setQueryString("select u from User as u " +
                                               "where u.updatedDate < :updatedDate and u.status = :status");

        Map<String, Object> map = new HashMap<>();
        LocalDateTime now = LocalDateTime.now();
        map.put("updatedDate", now.minusYears(1));
        map.put("status", UserStatus.ACTIVE);

        jpaPagingItemReader.setParameterValues(map);
        jpaPagingItemReader.setEntityManagerFactory(entityManagerFactory);
        jpaPagingItemReader.setPageSize(CHUNK_SIZE);
        return jpaPagingItemReader;
    }

//    @Bean
//    @StepScope
//    public ListItemReader<User> inactiveUserReader() {
//        List<User> oldUsers =
//            userRepository.findByUpdatedDateBeforeAndStatusEquals(LocalDateTime.now(), UserStatus.ACTIVE);
//        return new ListItemReader<>(oldUsers);
//    }

    public ItemProcessor<User, User> inactiveUserProcessor() {
        return User::setInactive;
    }

    public JpaItemWriter<User> inactiveUserWriter() {
        JpaItemWriter<User> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }
//    public ItemWriter<User> inactiveUserWriter() {
//        return userRepository::saveAll;
//    }
}
