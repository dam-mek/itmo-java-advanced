package info.kgeorgiy.ja.denisov.crawler;

import info.kgeorgiy.java.advanced.crawler.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static info.kgeorgiy.java.advanced.crawler.URLUtils.getHost;

public class WebCrawler implements NewCrawler {
    private final Downloader downloader;
    private final ExecutorService downloadExecutor;
    private final ExecutorService extractExecutor;
    private final int perHost;
    private final Map<String, SynchronizedQueue> hostQueues;

    private class SynchronizedQueue {
        private final Semaphore semaphore;
        private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

        private SynchronizedQueue(int host) {
            semaphore = new Semaphore(host);
        }

        public boolean isEmpty() {
            return queue.isEmpty();
        }

        synchronized void newTask(Runnable task) {
            if (semaphore.tryAcquire()) {
                downloadExecutor.submit(task);
            } else {
                queue.add(task);
            }
        }

        synchronized void leaveFromQueue() {
            Runnable task = queue.poll();
            if (task == null) {
                semaphore.release();
            } else {
                downloadExecutor.submit(task);
            }
        }
    }
    
    /**
     * Constructs a new WebCrawler instance with the specified parameters.
     *
     * @param downloader     the downloader to be used for downloading documents.
     * @param downloaders    the maximum number of threads for downloading documents concurrently.
     * @param extractors     the maximum number of threads for extracting links concurrently.
     * @param perHost        the maximum number of documents to be downloaded concurrently from the same host.
     */
    public WebCrawler(Downloader downloader, int downloaders, int extractors, int perHost) {
        this.downloader = downloader;
        this.downloadExecutor = Executors.newFixedThreadPool(downloaders);
        this.extractExecutor = Executors.newFixedThreadPool(extractors);
        this.perHost = perHost;
        hostQueues = new ConcurrentHashMap<>();
    }

    private SynchronizedQueue getHostQueue(String currentUrl, Map<String, IOException> errors) {
        String host;
        try {
            host = getHost(currentUrl);
        } catch (MalformedURLException e) {
            errors.put(currentUrl, e);
            return null;
        }
        return hostQueues.computeIfAbsent(host, hi -> new SynchronizedQueue(perHost));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result download(String url, int depth) {
        return download(url, depth, new HashSet<>());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result download(String url, int depth, Set<String> excludes) {
        final Map<String, IOException> errors = new ConcurrentHashMap<>();
        final Set<String> downloaded = ConcurrentHashMap.newKeySet();
        if (isExcludedUrl(url, excludes)) {
            return new Result(new ArrayList<>(downloaded), errors);
        }
        final Set<String> currentDepthUrls = ConcurrentHashMap.newKeySet();
        final Set<String> nextDepthUrls = ConcurrentHashMap.newKeySet();
        final Phaser phaser = new Phaser(1);

        currentDepthUrls.add(url);
        while (!currentDepthUrls.isEmpty() && depth-- > 0) {
            int finalDepth = depth;
            currentDepthUrls.forEach(
                    currentUrl -> {
                        SynchronizedQueue hostQueue = getHostQueue(currentUrl, errors);
                        if (hostQueue == null) {
                            return;
                        }
                        downloadUrl(
                                currentUrl, hostQueue,
                                downloaded, nextDepthUrls,
                                phaser, excludes,
                                errors, finalDepth
                        );
                    }
            );
            phaser.arriveAndAwaitAdvance();
            currentDepthUrls.clear();
            currentDepthUrls.addAll(nextDepthUrls);
            nextDepthUrls.clear();

            restoreHostQueues();
        }

        return new Result(new ArrayList<>(downloaded), errors);
    }

    private void restoreHostQueues() {
        int MAX_HOST_QUEUES = 2048;
        if (hostQueues.size() > MAX_HOST_QUEUES) {
            hostQueues.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        }
    }

    private void downloadUrl(
            String currentUrl,
            SynchronizedQueue hostQueue,
            Set<String> downloaded,
            Set<String> nextDepthUrls,
            Phaser phaser,
            Set<String> excludes,
            Map<String, IOException> errors,
            int depth
    ) {
            phaser.register();
            hostQueue.newTask(() -> {
                try {
                    if (downloaded.contains(currentUrl) || errors.containsKey(currentUrl)) {
                        return;
                    }
                    Document document;
                    try {
                        document = downloader.download(currentUrl);
                    } catch (IOException e) {
                        downloaded.remove(currentUrl);
                        errors.put(currentUrl, e);
                        return;
                    }
                    downloaded.add(currentUrl);
                    if (depth > 0) {
                        extractLinks(downloaded, nextDepthUrls, phaser, excludes, errors, document);
                    }
                } finally {
                    phaser.arriveAndDeregister();
                    hostQueue.leaveFromQueue();
                }
            });
    }

    private void extractLinks(
            Set<String> downloaded,
            Set<String> nextDepthUrls,
            Phaser phaser,
            Set<String> excludes,
            Map<String, IOException> errors,
            Document document
    ) {
        phaser.register();
        Predicate<String> isValidLink =
                link -> !(downloaded.contains(link) || errors.containsKey(link) || isExcludedUrl(link, excludes));
        extractExecutor.submit(() -> {
            List<String> links;
            try {
                links = document.extractLinks();
            } catch (IOException e) {
                return;
            }
            nextDepthUrls.addAll(links.stream().filter(isValidLink).collect(Collectors.toSet()));
            phaser.arriveAndDeregister();
        });
    }

    private boolean isExcludedUrl(String url, Set<String> excludes) {
        return excludes.stream().anyMatch(url::contains);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        downloadExecutor.shutdown();
        extractExecutor.shutdown();
    }

    /**
     * Entry point for starting the WebCrawler from the command line.
     * <p>
     * The command line arguments should follow the format:
     * {@code WebCrawler url [depth [downloads [extractors [perHost]]]]}.
     * <ul>
     * <li>{@code url} - the starting URL for crawling.</li>
     * <li>{@code depth} - the maximum depth for crawling, defaults to 1 if not provided.</li>
     * <li>{@code downloads} - the maximum number of concurrent download threads, defaults to 4 if not provided.</li>
     * <li>{@code extractors} - the maximum number of concurrent link extraction threads, defaults to 4 if not provided.</li>
     * <li>{@code perHost} - the maximum number of concurrent downloads per host, defaults to 2 if not provided.</li>
     * </ul>
     *
     * @param args the command line arguments.
     */
    public static void main(String[] args) {
        if (args.length < 1 || args.length > 5) {
            System.out.println("Usage: WebCrawler url [depth [downloads [extractors [perHost]]]]");
            return;
        }
        String url = args[0];
        try {
            int depth = args.length > 1 ? Integer.parseInt(args[1]) : 1;
            int downloaders = args.length > 2 ? Integer.parseInt(args[2]) : 4;
            int extractors = args.length > 3 ? Integer.parseInt(args[3]) : 4;
            int perHost = args.length > 4 ? Integer.parseInt(args[4]) : 2;
            try (WebCrawler crawler = new WebCrawler(new CachingDownloader(1), downloaders, extractors, perHost)) {
                Result result = crawler.download(url, depth);
                System.out.println("Downloaded: " + String.join("", result.getDownloaded()));
                System.out.println("Errors: ");
                for (Map.Entry<String, IOException> entry : result.getErrors().entrySet()) {
                    System.out.println(entry.getKey() + " -> " + entry.getValue());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (NumberFormatException e) {
            System.err.println("depth, downloads,  extractors and perHost should be integer");
        }

    }
}
