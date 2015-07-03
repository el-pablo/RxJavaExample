package com.moo.rxjava;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EntitlementEmitter implements Observable.OnSubscribe<String> {

    private final long delay;
    private String[] responses;

    public EntitlementEmitter(long delay, String... responses) {
        this.delay = delay;
        this.responses = responses;
    }

    @Override
    public void call(Subscriber<? super String> subscriber) {
        Scheduler scheduler = Schedulers.computation();
        Scheduler.Worker worker = scheduler.createWorker();
        subscriber.add(worker);
        AtomicInteger count = new AtomicInteger();
        worker.schedulePeriodically(() -> {
            int j = count.getAndIncrement();
            if(j < responses.length) {
                String response = responses[j];
                if(response.equals("ERROR")) {
                    subscriber.onError(new Exception("Blah"));
                } else {
                    subscriber.onNext(response);
                }
            } else {
                subscriber.onCompleted();
            }
        }, delay, 1000, MILLISECONDS);
    }

}
