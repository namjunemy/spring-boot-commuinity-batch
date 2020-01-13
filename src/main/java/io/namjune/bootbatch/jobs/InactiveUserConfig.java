package io.namjune.bootbatch.jobs;

import io.namjune.bootbatch.domain.User;
import io.namjune.bootbatch.domain.enums.UserStatus;
import io.namjune.bootbatch.jobs.readers.QueueItemReader;
import io.namjune.bootbatch.repository.UserRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;
import java.util.List;

@AllArgsConstructor
@Configuration
public class InactiveUserConfig {

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
            .<User, User>chunk(10)
            .reader(inactiveUserReader())
            .processor(inactiveUserProcessor())
            .writer(inactiveUserWriter())
            .build();
    }

    @Bean
    @StepScope
    public QueueItemReader<User> inactiveUserReader() {
        List<User> oldUsers =
            userRepository.findByUpdatedDateBeforeAndStatusEquals(LocalDateTime.now(), UserStatus.ACTIVE);
        return new QueueItemReader<>(oldUsers);
    }

    public ItemProcessor<User, User> inactiveUserProcessor() {
        return User::setInactive;
    }

    public ItemWriter<User> inactiveUserWriter() {
        return userRepository::saveAll;
    }
}
