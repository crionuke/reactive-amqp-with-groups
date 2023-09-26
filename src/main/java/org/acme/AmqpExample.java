package org.acme;

import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.time.Duration;
import java.util.Random;

@Startup
@ApplicationScoped
public class AmqpExample {

    final MutinyEmitter<Long> outboxEmitter;
    final Multi<Long> inboxEvents;

    public AmqpExample(@Channel("outbox-events") MutinyEmitter<Long> outboxEmitter,
                       @Channel("inbox-events") Multi<Long> inboxEvents) {
        this.outboxEmitter = outboxEmitter;
        this.inboxEvents = inboxEvents;

        // Consumer
        inboxEvents
                .onItem().transformToUniAndMerge(value -> Uni.createFrom().voidItem()
                        .invoke(voidItem -> System.out.println("Incoming value=" + value))
                        .onItem().delayIt().by(Duration.ofSeconds(new Random().nextInt(1, 10)))
                        .replaceWith(value)
                        .invoke(voidItem -> System.out.println("Handled value=" + value)))
                .subscribe().with(value -> System.out.println("Consumed value=" + value));
    }

    @Scheduled(every = "60s")
    Uni<Void> producer() {
        // Producer
        return Multi.createFrom().range(0, 10)
                .onItem().transformToUniAndConcatenate(value -> {
                    // Use the same group for all outgoing events.
                    // It should guarantee one by one handling on the side of consumer
                    final var metadata = OutgoingAmqpMetadata.builder()
                            .withGroupId("group")
                            .build();

                    final var message = Message.of(Long.valueOf(value))
                            .addMetadata(metadata);

                    return outboxEmitter.sendMessage(message);
                })
                .collect().asList()
                .replaceWithVoid();
    }
}
