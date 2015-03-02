package com.moo.rxjava;

import rx.Observable;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class Reactive {

    public static void main(String[] args) {
        Observable<String> cqsObservable = Observable.create(new EntitlementEmitter(0, "1_123_SEASON", "1_456_EPISODE", "1_789_RENTAL"));
        Observable<String> entitlementsObservable = Observable.create(new EntitlementEmitter(500, "1:321:SEASON", "1:654:EPISODE", "1:987:EPISODE"));

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

        merged.toBlocking().forEach(System.out::println);
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
