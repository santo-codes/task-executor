package opentext;

import java.util.UUID;
import java.util.concurrent.Callable;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        TaskExecutorImpl taskExecutor = new TaskExecutorImpl(2);
        var g1 = new TaskGroup(UUID.randomUUID());
        var g2 = new TaskGroup(UUID.randomUUID());
        var f1 = taskExecutor.submitTask(new Task<>(UUID.randomUUID(), g1, TaskType.READ, new TestTask("t1")));
        var f2 = taskExecutor.submitTask(new Task<>(UUID.randomUUID(), g2, TaskType.READ, new TestTask("t2")));
        var f3 = taskExecutor.submitTask(new Task<>(UUID.randomUUID(), g1, TaskType.READ, new TestTask("t3")));
        var f4 = taskExecutor.submitTask(new Task<>(UUID.randomUUID(), g2, TaskType.READ, new TestTask("t4")));
        var f5 = taskExecutor.submitTask(new Task<>(UUID.randomUUID(), g1, TaskType.READ, new TestTask("t5")));
        var f6 = taskExecutor.submitTask(new Task<>(UUID.randomUUID(), g2, TaskType.READ, new TestTask("t6")));

        Thread.sleep(6000);

        System.out.println(f1.isDone() + "-" + f2.isDone() + "-" + f3.isDone() + "-" + f4.isDone() + "-" + f5.isDone() + "-" + f6.isDone());

        taskExecutor.shutdown();
    }

    private record TestTask(String name) implements Callable<String> {

        @Override
        public String call() throws Exception {
            System.out.println("Starting " + name);
            Thread.sleep(1000);
            System.out.println("Completed " + name);
            return name;
        }
    }
}
