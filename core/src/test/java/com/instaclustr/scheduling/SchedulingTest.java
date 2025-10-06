package com.instaclustr.scheduling;

import com.instaclustr.measure.Time;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationRequest;
import org.junit.jupiter.api.Test;

public class SchedulingTest {

    @Test
    public void testScheduling() throws Exception {
        final Time time = new Time(5L, Time.TimeUnit.SECONDS);
        DaemonScheduler<MyRequest, MyOperation> scheduler = new DaemonScheduler<>(time, () -> new MyOperation(new MyRequest()));
        scheduler.setup();

        Thread thread = new Thread(scheduler::execute);

        thread.start();

        Thread.sleep(20000);

        scheduler.shutDown();
    }

    public static final class MyOperation extends Operation<MyRequest> {

        public MyOperation(final MyRequest request) {
            super(request);
        }

        @Override
        protected void run0() throws Exception {
            System.out.println("hello");
        }
    }

    public static final class MyRequest extends OperationRequest {

    }
}
