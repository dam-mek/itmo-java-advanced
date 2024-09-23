package info.kgeorgiy.ja.denisov.iterative;

import info.kgeorgiy.java.advanced.iterative.NewListIP;
import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IterativeParallelism implements NewListIP {
    final private ParallelMapper parallelMapper;

    /**
     * Creates a new instance of {@code IterativeParallelism} without using a mapper.
     */
    public IterativeParallelism() {
        this.parallelMapper = null;
    }

    /**
     * Creates a new instance of {@code IterativeParallelism} with the specified mapper.
     *
     * @param parallelMapper the mapper for parallel operation execution
     */
    public IterativeParallelism(ParallelMapper parallelMapper) {
        this.parallelMapper = parallelMapper;
    }

    private <T, R> R multiFunction(int threads, List<? extends T> values, Function<Stream<? extends T>, R> function, Function<Stream<R>, R> resultFunction, int step) throws InterruptedException {
        if (threads <= 0) {
            throw new IllegalArgumentException("number of concurrent threads must be positive");
        }

        List<Thread> threadList = new ArrayList<>();
        int size = (values.size() + step - 1) / step;

        threads = Math.min(threads, size);
        int chunkSize = size / threads;


        List<R> resultList;

        if (parallelMapper == null) {
            resultList = new ArrayList<>(Collections.nCopies(threads, null));
            int start = 0;
            for (int i = 0; i < threads; i++) {
                int end = start + chunkSize;
                if (i < size % threads) {
                    end++;
                }
                int finalI = i;
                int finalStart = start;
                int finalEnd = end;
                Thread thread = new Thread(() -> {
                    List<T> subList = new ArrayList<>();
                    for (int j = finalStart * step; j < finalEnd * step; j += step) {
                        subList.add(values.get(j));
                    }
                    resultList.set(finalI, function.apply(subList.stream())
                    );
                });
                thread.start();
                threadList.add(thread);
                start = end;
            }

            for (Thread thread : threadList) {
                thread.join();
                // :NOTE: не дожидаемся завершения
            }
        } else {
            List<Stream<? extends T>> portions = new ArrayList<>(Collections.nCopies(threads, null));
            int start = 0;
            for (int i = 0; i < threads; i++) {
                int end = start + chunkSize;
                if (i < size % threads) {
                    end++;
                }
                // :NOTE: явно есть общий код с предыдущей веткой if
                List<T> subList = new ArrayList<>();
                for (int j = start * step; j < end * step; j += step) {
                    subList.add(values.get(j));
                }
                portions.set(i, subList.stream());
                start = end;
            }
            resultList = parallelMapper.map(function, portions);
        }

        return resultFunction.apply(resultList.stream());
    }

    /**
     * Joins the elements of a list into a string.
     *
     * @param threads the number of threads
     * @param values the list of elements
     * @param step the step with which elements of the list are selected (starting from 0)
     * @return a string containing the joined elements of the list
     * @throws InterruptedException if the thread is interrupted during operation execution
     */
    @Override
    public String join(int threads, List<?> values, int step) throws InterruptedException {
        return multiFunction(
                threads, values,
                stream -> stream.map(Object::toString).collect(Collectors.joining()),
                stream -> stream.collect(Collectors.joining()),
                step
        );
    }

    /**
     * Filters the elements of a list based on a predicate.
     *
     * @param threads the number of threads
     * @param values the list of elements
     * @param predicate the predicate for filtering
     * @param step the step with which elements of the list are selected (starting from 0)
     * @return a list of elements that satisfy the predicate condition
     * @throws InterruptedException if the thread is interrupted during operation execution
     */
    @Override
    public <T> List<T> filter(int threads, List<? extends T> values, Predicate<? super T> predicate, int step) throws InterruptedException {
        return multiFunction(
                threads, values,
                stream -> stream.filter(predicate).collect(Collectors.toList()),
                stream -> stream.flatMap(Collection::stream).collect(Collectors.toList()),
                step
        );
    }

    /**
     * Applies a function to each element of a list.
     *
     * @param threads the number of threads
     * @param values the list of elements
     * @param f the function for transforming elements
     * @param step the step with which elements of the list are selected (starting from 0)
     * @return a list of elements transformed by the function
     * @throws InterruptedException if the thread is interrupted during operation execution
     */
    @Override
    public <T, U> List<U> map(int threads, List<? extends T> values, Function<? super T, ? extends U> f, int step) throws InterruptedException {
        return multiFunction(
                threads, values,
                stream -> stream.map(f).collect(Collectors.toList()),
                stream -> stream.flatMap(Collection::stream).collect(Collectors.toList()),
                step
        );
    }

    /**
     * Finds the maximum element in a list.
     *
     * @param threads the number of threads
     * @param values the list of elements
     * @param comparator the comparator for comparing elements
     * @param step the step with which elements of the list are selected (starting from 0)
     * @return the maximum element of the list
     * @throws InterruptedException if the thread is interrupted during operation execution
     */
    @Override
    public <T> T maximum(int threads, List<? extends T> values, Comparator<? super T> comparator, int step) throws InterruptedException {
        return multiFunction(
                threads, values,
                stream -> stream.max(comparator).orElse(null), // :NOTE: лучше orElseThrown
                stream -> stream.max(comparator).orElse(null),
                step
        );
    }

    /**
     * Finds the minimum element in a list.
     *
     * @param threads the number of threads
     * @param values the list of elements
     * @param comparator the comparator for comparing elements
     * @param step the step with which elements of the list are selected (starting from 0)
     * @return the minimum element of the list
     * @throws InterruptedException if the thread is interrupted during operation execution
     */
    @Override
    public <T> T minimum(int threads, List<? extends T> values, Comparator<? super T> comparator, int step) throws InterruptedException {
        return maximum(threads, values, comparator.reversed(), step);
    }

    /**
     * Checks if all elements of a list satisfy the given predicate.
     *
     * @param threads the number of threads
     * @param values the list of elements
     * @param predicate the predicate to apply to each element
     * @param step the step with which elements of the list are selected (starting from 0)
     * @return {@code true} if all elements satisfy the predicate, otherwise {@code false}
     * @throws InterruptedException if the thread is interrupted during operation execution
     */
    @Override
    public <T> boolean all(int threads, List<? extends T> values, Predicate<? super T> predicate, int step) throws InterruptedException {
        return multiFunction(
                threads, values,
                stream -> stream.allMatch(predicate),
                stream -> stream.allMatch(a -> a),
                step
        );
    }

    /**
     * Checks if any element of a list satisfies the given predicate.
     *
     * @param threads the number of threads
     * @param values the list of elements
     * @param predicate the predicate to apply to each element
     * @param step the step with which elements of the list are selected (starting from 0)
     * @return {@code true} if any element satisfies the predicate, otherwise {@code false}
     * @throws InterruptedException if the thread is interrupted during operation execution
     */
    @Override
    public <T> boolean any(int threads, List<? extends T> values, Predicate<? super T> predicate, int step) throws InterruptedException {
        // :NOTE: можно через any
        return multiFunction(threads, values,
                stream -> stream.anyMatch(predicate),
                stream -> stream.anyMatch(a -> a),
                step
        );
    }

    /**
     * Counts the number of elements in a list that satisfy the given predicate.
     *
     * @param threads the number of threads
     * @param values the list of elements
     * @param predicate the predicate to apply to each element
     * @param step the step with which elements of the list are selected (starting from 0)
     * @return the number of elements satisfying the predicate
     * @throws InterruptedException if the thread is interrupted during operation execution
     */
    @Override
    public <T> int count(int threads, List<? extends T> values, Predicate<? super T> predicate, int step) throws InterruptedException {
        return multiFunction(threads, values,
                stream -> Math.toIntExact(stream.filter(predicate).count()),
                stream -> stream.mapToInt(Integer::intValue).sum(),
                step
        );
    }
}
