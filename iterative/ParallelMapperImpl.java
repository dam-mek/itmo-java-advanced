package info.kgeorgiy.ja.denisov.iterative;

import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.*;
import java.util.function.Function;

/**
 * Implementation of the {@link ParallelMapper} interface that performs parallel mapping over list.
 */
public class ParallelMapperImpl implements ParallelMapper {
    private final List<Thread> workerThreads = new ArrayList<>();
    private final synchronizedQueue taskQueue = new synchronizedQueue();
    private volatile boolean closed = false;

    // :NOTE: naming + is it really synchronized? + generify
    private static class synchronizedQueue {
        private final Queue<Task> queue = new ArrayDeque<>();

        public boolean isEmpty() {
            return queue.isEmpty();
        }

        public Task getTask() {
            return queue.poll();
        }

        public void add(Task task) {
            queue.add(task);
        }
    }

    private static class Counter {
        private int count;
        private final int exceptedCount;

        Counter(int exceptedCount) {
            this.exceptedCount = exceptedCount;
        }

        boolean countReached() {
            return count >= exceptedCount;
        }
    }

    private record Task(Counter counter, Runnable task) {
    }

    private Task awaitTask() throws InterruptedException {
        synchronized (taskQueue) {
            while (taskQueue.isEmpty() && !closed) {
                try {
                    taskQueue.wait();
                } catch (InterruptedException e) {
                    if (closed) {
                        return null;
                    }
                }
            }
            if (!taskQueue.isEmpty()) {
                return taskQueue.getTask();
            } else {
                return null;
            }
        }
    }

    /**
     * Constructs a ParallelMapperImpl object with the specified number of threads.
     *
     * @param threads the number of worker threads to be created
     */
    public ParallelMapperImpl(int threads) {
        // :NOTE: use streams
        for (int i = 0; i < threads; i++) {
            // :NOTE: reuse worker
            Thread thread = new Thread(() -> {
                while (!Thread.interrupted()) {
                    try {
                        Task task = awaitTask();
                        if (task == null) {
                            return;
                        }

                        try {
                            task.task.run();
                        } catch (RuntimeException ignored) {
                            // :NOTE: should be re-throw to used in map()
                        }

                        synchronized (task.counter) {
                            task.counter.count++;
                            if (task.counter.countReached()) {
                                task.counter.notify();
                            }
                        }
                    } catch (InterruptedException e) {
                        // :NOTE: should be ignored. while cycle stopped
                        throw new RuntimeException(e);
                    }
                }
            });
            workerThreads.add(thread);
            thread.start();
        }
    }

    /**
     * Maps the specified function to each element in the input list and returns a list of results.
     *
     * @param f    the function to apply to each element
     * @param args the list of input elements
     * @param <T>  the type of input elements
     * @param <R>  the type of output elements
     * @return a list of results obtained by applying the function to each input element
     * @throws InterruptedException  if the current thread is interrupted while waiting
     *                               for the mapping operation to complete
     * @throws IllegalStateException if the parallel mapper is closed
     */
    @Override
    public <T, R> List<R> map(Function<? super T, ? extends R> f, List<? extends T> args) throws InterruptedException {
        if (closed) {
            throw new IllegalStateException("parallelMapper is closed");
        }

        List<R> results = new ArrayList<>(Collections.nCopies(args.size(), null));
        final Counter counter = new Counter(args.size());
        for (int i = 0; i < args.size(); i++) {
            int finalI = i;
            synchronized (taskQueue) {
                taskQueue.add(new Task(counter, () -> results.set(finalI, f.apply(args.get(finalI)))));
                taskQueue.notifyAll(); // :NOTE: notify()
            }
        }

        // :NOTE: counter is local var
        synchronized (counter) {
            while (!counter.countReached()) {
                counter.wait();
            }
        }
        return results;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // :NOTE: map will freeze
        closed = true;
        for (Thread workerThread : workerThreads) {
            workerThread.interrupt();
        }
        for (Thread workerThread : workerThreads) {
            // :NOTE: thread leak
            try {
                workerThread.join();
            } catch (InterruptedException ignored) { // :NOTE: +theory
            }
        }
    }
}
