package com.moo.rxjava;

import rx.Observable;
import rx.Scheduler;
import rx.observables.BlockingObservable;
import rx.schedulers.ImmediateScheduler;
import rx.schedulers.NewThreadScheduler;
import rx.schedulers.Schedulers;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class Reactive {

    public static void main(String[] args) {
        Reactive reactive = new Reactive();
        reactive.testErrors();
    }

    public void standard() {
        long start = System.currentTimeMillis();
        Scheduler scheduler = Schedulers.newThread();

        Observable<String> cqsObservable = Observable.create(new EntitlementEmitter(0, "1_123_SEASON", "1_456_EPISODE", "1_789_RENTAL"));
        Observable<String> entitlementsObservable = Observable.create(new EntitlementEmitter(500, "1:321:SEASON", "1:654:EPISODE", "1:987:MOVIE"));

        Observable<String> merged = cqsObservable.map(e -> e.replaceAll("_", ":")).mergeWith(entitlementsObservable);

        merged = merged.buffer(4).flatMap(entitlementList -> {
            List<Integer> ids = entitlementList
                    .stream()
                    .map(e -> Integer.valueOf(e.split(":")[1]))
                    .collect(toList());

            List<String> titlesFromFeedService = getTitlesFromFeedService(ids);

            List<String> concatenated = range(0, entitlementList.size())
                    .mapToObj(i -> entitlementList.get(i) + ":" + titlesFromFeedService.get(i))
                    .collect(toList());

            return Observable.from(concatenated);
        });

        BlockingObservable<String> blocking = merged.toBlocking();
        blocking.forEach(System.out::println);
        System.out.println(System.currentTimeMillis() - start);
    }

    public void testErrors() {
        long start = System.currentTimeMillis();

        Observable<String> cqsObservable = Observable.create(new EntitlementEmitter(0, "1_123_SEASON", "1_456_EPISODE", "1_789_RENTAL"));
        Observable<String> entitlementsObservable = Observable.create(new EntitlementEmitter(500, "1:321:SEASON", "1:654:EPISODE", "ERROR"));

        Observable<String> merged = cqsObservable.map(e -> e.replaceAll("_", ":")).mergeWith(entitlementsObservable);

        merged = merged.buffer(4).flatMap(entitlementList -> {
            List<Integer> ids = entitlementList
                    .stream()
                    .map(e -> Integer.valueOf(e.split(":")[1]))
                    .collect(toList());

            List<String> titlesFromFeedService = getTitlesFromFeedService(ids);

            List<String> concatenated = range(0, entitlementList.size())
                    .mapToObj(i -> entitlementList.get(i) + ":" + titlesFromFeedService.get(i))
                    .collect(toList());

            return Observable.from(concatenated);
        });

//        Observable<String> handlesErrors = merged.doOnError(e -> System.out.println("error: " + e.getMessage())).retry(3).single();
        Observable<String> handlesErrors = merged.doOnError(e -> System.out.println("error: " + e.getMessage())).retry(3).onErrorReturn(ex -> "Default");
        BlockingObservable<String> blocking = handlesErrors.toBlocking();
        blocking.forEach(System.out::println);
        System.out.println(System.currentTimeMillis() - start);
    }

    public void testConcurrency() {
        try {
            Observable<String> observableOne = Observable.create(observer -> {
                try {
                    System.out.println("Observing1: Now waiting...."+Thread.currentThread().getName());
                    Thread.sleep(1000);
                    System.out.println("Observing1: Finished waiting...");
                    for (int i = 0; i < 1000; i++) {
                        observer.onNext("a" + i);
                        Thread.sleep(10);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                observer.onCompleted();
            });

            Observable<String> observableTwo = Observable.create(observer -> {
                try {
                    System.out.println("Observing2: Now waiting...."+Thread.currentThread().getName());
                    Thread.sleep(1000);
                    System.out.println("Observing2: Finished waiting...");
                    for (int i = 0; i < 1000; i++) {
                        observer.onNext("b" + i);
                        Thread.sleep(10);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                observer.onCompleted();
            });

            System.out.println("Subscribing...");

            observableOne = observableOne.subscribeOn(Schedulers.newThread()).onBackpressureBuffer();
            observableTwo = observableTwo.subscribeOn(Schedulers.newThread()).onBackpressureBuffer();
            Thread.sleep(5000);
            Observable<String> merged = observableOne.mergeWith(observableTwo);
            System.out.println("Subscribed: waiting...");
            merged.subscribe(System.out::println);
            Thread.sleep(5000);
            System.out.println("Finished Subscribe:waiting...");

            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    public static String getTitleFromFeedService(int id){
        switch (id) {
            case 123: return "The Big Lebowski";
            case 456: return "Fear and Loathing";
            case 789: return "Delicatessen";
            case 321: return "Batman";
            case 654: return "Trainspotting";
            case 987: return "Mallrats";
            default: return "Unknown";
        }

    }

    public static List<String> getTitlesFromFeedService(List<Integer> ids){
        System.out.println("FeedService Batch size: "+ids.size());
        List<String> titles = new ArrayList<>();
        for(int i : ids){
            titles.add(getTitleFromFeedService(i));
        }
        return titles;
    }

}
