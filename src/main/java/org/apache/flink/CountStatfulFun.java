package org.apache.flink;


import org.apache.flink.count.CountRequest;
import org.apache.flink.count.CountResponse;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

/**
 * A stateful function that generates a unique greeting for each user based on how many times that
 * user has been seen by the system.
 */
final class CountStatfulFun implements StatefulFunction {

    /**
     * The function type is the unique identifier that identifies this type of function. The type, in
     * conjunction with an identifier, is how routers and other functions can use to reference a
     * particular instance of a greeter function.
     *
     * <p>If this was a multi-module application, the function type could be in different package so
     * functions in other modules could message the greeter without a direct dependency on this class.
     */
    static final FunctionType TYPE = new FunctionType("apache", "greeter");

    /**
     * The persisted value for maintaining state about a particular user. The value returned by this
     * field is always scoped to the current user. seenCount is the number of times the user has been
     * greeted.
     */
    @Persisted
    private final PersistedValue<Integer> Count = PersistedValue.of("count", Integer.class);
    @Persisted
    private final PersistedValue<Integer> MaxById = PersistedValue.of("MaxById", Integer.class);

    @Override
    public void invoke(Context context, Object input) {
        CountRequest greetMessage = (CountRequest) input;
        CountResponse response = computePersonalizedGreeting(greetMessage);
        context.send(CountIO.GREETING_EGRESS_ID, response);
    }

    private CountResponse computePersonalizedGreeting(CountRequest Message) {
        final String id = Message.getId();
        final int money = Message.getMoney();

        int count = Count.getOrDefault(1);
        Count.set(count + 1);

        int max = MaxById.getOrDefault(0);

        if (Integer.valueOf(money) > max)
        {
            max = Integer.valueOf(money) ;
            MaxById.set(max);
        }

        String result = String.format("id:%s  count is %s, max is %s ", id, count, max);

        return CountResponse.newBuilder().setId(id).setResult(result).build();
    }

}
