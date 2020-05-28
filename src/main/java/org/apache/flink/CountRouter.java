package org.apache.flink;

import org.apache.flink.count.CountRequest;
import org.apache.flink.statefun.sdk.io.Router;

final class CountRouter implements Router<CountRequest> {

    @Override
    public void route(CountRequest message, Downstream<CountRequest> downstream) {
        downstream.forward(CountStatfulFun.TYPE, message.getId(), message);
    }
}

