package info.kgeorgiy.ja.denisov.walk;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.EnumSet;

public class BaseWalk {
    private final int depth;
    private final String walkerName;

    public BaseWalk(int depth) {
        this(depth, "BaseWalk");
    }

    public BaseWalk(int depth, String walkerName) {
        this.depth = depth;
        this.walkerName = walkerName;
    }

    public String error(String message) {
        return walkerName + "Error: " + message;
    }

    public void walk(String[] args) throws WalkException {
        if (args == null) {
            throw new WalkException(error("args is null"));
        }
        if (args.length != 2) {
            throw new WalkException(error("invalid number of inputs. Expected input file and output file"));
        }
        if (args[0] == null || args[1] == null) {
            throw new WalkException(error("filename is null"));
        }

        Path inputPath, outputPath;
        try {
            inputPath = Paths.get(args[0]);
        } catch (InvalidPathException e) {
            throw new WalkException(error("input file not found"), e);
        }
        try {
            outputPath = Paths.get(args[1]);
        } catch (InvalidPathException e) {
            throw new WalkException(error("output file not found"), e);
        }
        walk(inputPath, outputPath);
    }

    public void walk(Path inputPath, Path outputPath) throws WalkException {
        Path parentOutputPath = outputPath.getParent();
        if (parentOutputPath != null) {
            try {
                Files.createDirectories(parentOutputPath);
            } catch (IOException e) {
                System.err.println(error("can't create output dirs"));
            }
        }

        try (BufferedReader inputReader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8)) {
            try (BufferedWriter outputWriter = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
                WalkVisitor visitor = new WalkVisitor(outputWriter);
                String filepath;
                while ((filepath = inputReader.readLine()) != null) {
                    Path path;
                    try {
                        path = Paths.get(filepath);
                    } catch (InvalidPathException e) {
                        visitor.writeInvalidPath(filepath, e);
                        continue;
                    }
                    Files.walkFileTree(path, EnumSet.noneOf(FileVisitOption.class), depth, visitor);
                }
            } catch (IOException e) {
                throw new WalkException(error("can't open output file"), e);
            }
        } catch (IOException e) {
            throw new WalkException(error("can't open input file"), e);
        }
    }
}