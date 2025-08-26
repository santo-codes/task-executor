package taskExecutor;

import java.util.Comparator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Assumption: Ordering is required only within a TaskGroup.
 */
public class TaskExecutorImpl implements TaskExecutor {

    private final ExecutorService threadPool;

    private final ConcurrentHashMap<TaskGroup, ConcurrentLinkedQueue<TaskWrapper<?>>> runnableTasksByTaskGroup;

    private final AtomicLong token = new AtomicLong();

    public TaskExecutorImpl(int maxConcurrency) {
        threadPool = Executors.newFixedThreadPool(maxConcurrency);
        runnableTasksByTaskGroup = new ConcurrentHashMap<>();
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        var currentToken = token.getAndIncrement();
        var wrapper = new TaskWrapper<>(task, runnableTasksByTaskGroup, currentToken);
        var tasks = runnableTasksByTaskGroup.computeIfAbsent(task.taskGroup(), taskGroup -> new ConcurrentLinkedQueue<>());
        tasks.add(wrapper);
        return threadPool.submit(wrapper);
    }

    public void shutdown() {
        threadPool.shutdownNow();
    }

    public static class TaskWrapper<T> implements Callable<T> {

        private final Task<T> task;
        private final ConcurrentHashMap<TaskGroup, ConcurrentLinkedQueue<TaskWrapper<?>>> runnableTasksByTaskGroup;
        private final Long token;
        private final CountDownLatch latch = new CountDownLatch(1);

        public TaskWrapper(Task<T> task, ConcurrentHashMap<TaskGroup, ConcurrentLinkedQueue<TaskWrapper<?>>> runnableTasksByTaskGroup, Long token) {
            this.task = task;
            this.runnableTasksByTaskGroup = runnableTasksByTaskGroup;
            this.token = token;
        }

        @Override
        public T call() throws Exception {
            for(TaskWrapper<?> earliestTask = getEarliestTask(); earliestTask != this; earliestTask = getEarliestTask()) {
                earliestTask.latch.await();
            }
            try {
                return task.taskAction().call();
            } finally {
                runnableTasksByTaskGroup.get(task.taskGroup()).remove(this);
                latch.countDown();
            }
        }

        private TaskWrapper<?> getEarliestTask() {
            var groupTasks = runnableTasksByTaskGroup.get(task.taskGroup());
            return groupTasks.stream().min(Comparator.comparing(o -> o.token)).orElseThrow();
        }
    }
}
