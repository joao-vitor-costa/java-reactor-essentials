package reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

@Slf4j
/**
 * Reactive Streams
 * 1 - Asynchronous
 * 2 - Non-blocking
 * 3 - Backpressure
 *
 * Publisher <- (subscribe) Subscriber
 * Subscription is created
 * Publisher (onSubscribe with the subscriptions) -> Subscriber
 * Subscription <- (request N) Subscriber
 * Publisher -> (onNext) Subscriber
 * until:
 * 1 - Publisher sends all the objects requested.
 * 2 - Publisher sends all the objects it has. (onComplete) subscriber and subscription will be canceled
 * 3 - There is an error. (onError) -> subscriber and subscription will be canceled
 */
public class MonoTest {

    @BeforeAll
    public static void setUp() {
        BlockHound.install();
    }

    @Test
    public void blockHoundWorks() {
        try {
            FutureTask<?> task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });
            Schedulers.parallel().schedule(task);

            task.get(10, TimeUnit.SECONDS);
            Assertions.fail("should fail");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }
    }

    @Test
    public void monoSubscriber() {
        String name = "John";
        Mono<String> mono = Mono.just(name)
            .log();

        mono.subscribe();

        log.info("-------------------------------------------------------------");

        StepVerifier.create(mono)
            .expectNext(name)
            .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumer() {
        String name = "John";
        Mono<String> mono = Mono.just(name)
            .log();

        mono.subscribe(s -> log.info("Value {}", s));

        log.info("-------------------------------------------------------------");

        StepVerifier.create(mono)
            .expectNext(name)
            .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerError() {
        String name = "John";
        Mono<String> mono = Mono.just(name)
            .map(s -> {throw new RuntimeException("Testing mono with error");});

        mono.subscribe(s -> log.info("Name {}", s), s -> log.error("Something bad happened"));
        mono.subscribe(s -> log.info("Name {}", s), Throwable::printStackTrace);

        log.info("-------------------------------------------------------------");

        StepVerifier.create(mono)
            .expectError(RuntimeException.class)
            .verify();
    }

    @Test
    public void monoSubscriberConsumerComplete() {
        String name = "John";
        Mono<String> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value {}", s),
            Throwable::printStackTrace,
            () -> log.info("FINISHED"));

        log.info("-------------------------------------------------------------");

        StepVerifier.create(mono)
            .expectNext(name.toUpperCase())
            .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerSubscription() {
        String name = "John";
        Mono<String> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value {}", s),
            Throwable::printStackTrace,
            () -> log.info("FINISHED"),
            Subscription::cancel);

        log.info("-------------------------------------------------------------");
    }

    @Test
    public void monoSubscriberConsumerSubscriptionRequest() {
        String name = "John";
        Mono<String> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value {}", s),
            Throwable::printStackTrace,
            () -> log.info("FINISHED"),
            subscription -> subscription.request(5));

        log.info("-------------------------------------------------------------");

        StepVerifier.create(mono)
            .expectNext(name.toUpperCase())
            .verifyComplete();
    }

    @Test
    public void monoDoOnMethods() {
        String name = "John";
        Mono<Object> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase)
            .doOnSubscribe(subscription -> log.info("subscribed"))
            .doOnRequest(longNumber -> log.info("Request Received, starting doing something...."))
            .doOnNext(s -> log.info("Value is here. Executing doOnNext {}", s))
            .flatMap(s -> Mono.empty())
            .doOnNext(s -> log.info("Value is here. Executing doOnNext {}", s))
            .doOnSuccess(s -> log.info("doOnSuccess executed {}", s));

        mono.subscribe(s -> log.info("Value {}", s),
            Throwable::printStackTrace,
            () -> log.info("FINISHED"));

        log.info("-------------------------------------------------------------");
    }

    @Test
    public void monoDoOnError() {
        Mono<Object> error = Mono.error(new IllegalAccessException("Illegal argument exception"))
            .doOnError(e -> MonoTest.log.error("Error message: {}", e.getMessage()))
            .doOnNext(s -> log.info("Executing this doOnNext"))
            .log();

        log.info("-------------------------------------------------------------");

        StepVerifier.create(error)
            .expectError(IllegalAccessException.class)
            .verify();
    }

    @Test
    public void monoDoOnErrorResume() {
        String name = "John";
        Mono<Object> error = Mono.error(new IllegalAccessException("Illegal argument exception"))
            .doOnError(e -> MonoTest.log.error("Error message: {}", e.getMessage()))
            .onErrorResume(s -> {
                log.info("Inside On Error Resume");
                return Mono.just(name);
            })
            .log();

        log.info("-------------------------------------------------------------");

        StepVerifier.create(error)
            .expectNext(name)
            .verifyComplete();
    }

    @Test
    public void monoDoOnErrorReturn() {
        String name = "John";
        Mono<Object> error = Mono.error(new IllegalAccessException("Illegal argument exception"))
            .onErrorReturn("EMPTY")
            .log();

        log.info("-------------------------------------------------------------");

        StepVerifier.create(error)
            .expectNext("EMPTY")
            .verifyComplete();
    }

}
