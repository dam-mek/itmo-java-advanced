package info.kgeorgiy.ja.denisov.implementor;

import info.kgeorgiy.java.advanced.implementor.JarImpler;
import info.kgeorgiy.java.advanced.implementor.ImplerException;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Function;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 * Class for implementing interfaces by generating a class
 * that extends the provided interface.
 */
public class Implementor implements JarImpler {
    /**
     * Constant representing the string "argument".
     */
    private static final String ARGUMENT = "argument";

    /**
     * Constant representing the system-dependent line separator.
     */
    private static final String LINE_SEP = System.lineSeparator();

    /**
     * Constant representing the system-dependent file separator.
     */
    private static final String FILE_SEP = File.separator;

    /**
     * Constant representing the string "    " (four spaces).
     */
    private static final String TAB = "    ";

    /**
     * Main method for running the Implementor.
     * Takes in command-line arguments to determine behavior.
     * @param args Command-line arguments. If length is 1, attempts to implement the class provided as the argument.
     * If length is 3 and the first argument is "-jar", attempts to implement the class provided as the second
     * argument and generate a JAR file at the location specified by the third argument.
     */
    public static void main(String[] args) {
        try {
            if (args.length == 1) {
                new Implementor().implement(Class.forName(args[0]), Path.of("."));
            } else if (args.length == 3 && Objects.equals(args[0], "-jar")) {
                new Implementor().implementJar(Class.forName(args[1]), Path.of(args[2]));
            } else {
                System.err.println("Invalid number of arguments");
            }
        } catch (ImplerException e) {
            System.err.println("error while implement");
        } catch (ClassNotFoundException e) {
            System.err.println("Invalid class");
        }
    }

    /**
     * Implements the provided interface or abstract class.
     * Generates a class file implementing the interface/abstract class
     * and saves it to the specified root directory.
     * @param token The interface/abstract class to implement.
     * @param root The root directory to save the generated class file.
     * @throws ImplerException If implementation fails due to any reason.
     */
    @Override
    public void implement(Class<?> token, Path root) throws ImplerException {
        checkClassToken(token);
        String fullPath = replaceInPackageName(token);

        Path path = root.resolve(Paths.get(fullPath, token.getSimpleName() + "Impl.java"));
        try {
            Files.createDirectories(path.getParent());
        } catch (IOException e) {
            System.err.println("Exception while create directories");
        }
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writePackage(token, writer);
            writer.newLine();
            writer.newLine();
            writeClass(token, writer);
        } catch (IOException e) {
            throw new ImplerException("Error with writing to BufferedWriter", e);
        }
    }

    /**
     * Checks if the provided class token can be implemented.
     * Throws ImplerException if it cannot be implemented.
     * @param token The class token to check.
     * @throws ImplerException If the class token cannot be implemented.
     */
    private static void checkClassToken(Class<?> token) throws ImplerException {
        if (token.isPrimitive()) {
            throw new ImplerException("can't implement primitive type");
        }
        if (token.equals(String.class)) {
            throw new ImplerException("can't implement String");
        }
        if (token.equals(Enum.class)) {
            throw new ImplerException("can't implement Enum");
        }
        if (token.equals(Integer.class)) {
            throw new ImplerException("can't implement Integer");
        }
        if (token.equals(Record.class)) {
            throw new ImplerException("can't implement Record");
        }
        if (token.isArray()) {
            throw new ImplerException("can't implement array");
        }
        if (token.isAnonymousClass()) {
            throw new ImplerException("can't implement anonymous class");
        }
        if (Modifier.isPrivate(token.getModifiers())) {
            throw new ImplerException("Can't implement private class");
        }
        if (Modifier.isFinal(token.getModifiers())) {
            throw new ImplerException("Can't implement final class");
        }
    }

    /**
     * Writes the class definition to the output file.
     * Includes class name, constructors, and methods.
     * @param token The interface/abstract class to implement.
     * @param writer The BufferedWriter to write the class definition.
     * @throws IOException If an I/O error occurs while writing to the output file.
     * @throws ImplerException If there is an error in generating the class definition.
     */
    private void writeClass(Class<?> token, BufferedWriter writer) throws IOException, ImplerException {
        writeClassName(token, writer);
        writer.write("{");
        writer.newLine();
        writeConstructors(token, writer);
        writeMethods(token, writer);
        writer.write("}");
        writer.newLine();
    }

    /**
     * Writes the package declaration to the output file if the provided class token is not in the default package.
     * @param token The class token for which the package declaration is written.
     * @param writer The BufferedWriter to write the package declaration.
     * @throws IOException If an I/O error occurs while writing to the output file.
     */
    private void writePackage(Class<?> token, BufferedWriter writer) throws IOException {
        if (!token.getPackageName().isEmpty()) {

            writer.write("package " + token.getPackageName() + ";");
        }
    }

    /**
     * Writes the class name declaration to the output file, including whether it implements or extends another class/interface.
     * @param token The class token for which the class name declaration is written.
     * @param writer The BufferedWriter to write the class name declaration.
     * @throws IOException If an I/O error occurs while writing to the output file.
     */
    private void writeClassName(Class<?> token, BufferedWriter writer) throws IOException {
        writer.write("public class " + covertToUnicode(token.getSimpleName()) + "Impl ");
        if (token.isInterface()) {
            writer.write("implements");
        } else {
            writer.write("extends");
        }
        writer.write(" " + covertToUnicode(token.getCanonicalName()));
    }

    /**
     * Writes constructors of the provided class token to the output file.
     * @param token The class token for which constructors are written.
     * @param writer The BufferedWriter to write the constructors.
     * @throws IOException If an I/O error occurs while writing to the output file.
     * @throws ImplerException If there is an error in generating the constructors.
     */
    private void writeConstructors(Class<?> token, BufferedWriter writer) throws IOException, ImplerException {
        List<Constructor<?>> constructors = Arrays.stream(token.getDeclaredConstructors())
                .filter(constructor -> !Modifier.isPrivate(constructor.getModifiers()))
                .toList();
        if (constructors.isEmpty() && !token.isInterface()) {
            throw new ImplerException("Only private constructors");
        }
        for (Constructor<?> constructor : constructors) {
            addConstructor(token.getSimpleName(), constructor, writer);
        }
    }

    /**
     * Adds a constructor to the output file.
     * @param simpleName The simple name of the class.
     * @param constructor The constructor to add.
     * @param writer The BufferedWriter to write the constructor.
     * @throws IOException If an I/O error occurs while writing to the output file.
     */
    private void addConstructor(String simpleName, Constructor<?> constructor, BufferedWriter writer) throws IOException {
        StringBuilder methodSB = getConstructorSignature(simpleName, constructor);

        methodSB.append("{").append(LINE_SEP)
                .append(TAB).append(TAB).append("super(").append(getArguments(constructor.getParameterTypes())).append(");").append(LINE_SEP)
                .append(TAB).append("}").append(LINE_SEP);
        writer.write(methodSB.toString());
    }

    /**
     * Writes abstract methods of the provided class token to the output file.
     * @param token The class token for which abstract methods are written.
     * @param writer The BufferedWriter to write the abstract methods.
     * @throws IOException If an I/O error occurs while writing to the output file.
     */
    private void writeMethods(Class<?> token, BufferedWriter writer) throws IOException {
        Set<Method> methods = new TreeSet<>(this::compareMethods);
        methods.addAll(Arrays.asList(token.getMethods()));
        for (Class<?> clazz = token; clazz.getSuperclass() != null; clazz = clazz.getSuperclass()) {
            methods.addAll(Arrays.asList(clazz.getDeclaredMethods()));
        }
        for (Method method : methods) {
            if (Modifier.isAbstract(method.getModifiers())) {
                addMethod(method, writer);
            }
        }
    }


    /**
     * Compares two methods based on their names and parameter types.
     * @param m1 The first method to compare.
     * @param m2 The second method to compare.
     * @return A negative integer, zero, or a positive integer if the first method is less than, equal to, or greater than the second.
     */
    private int compareMethods(Method m1, Method m2) {
        if (!m1.getName().equals(m2.getName())) {
            return m1.getName().compareTo(m2.getName());
        }

        List<String> typeNames1 = Arrays.stream(m1.getParameterTypes()).map(Class::getCanonicalName).toList();
        List<String> typeNames2 = Arrays.stream(m2.getParameterTypes()).map(Class::getCanonicalName).toList();

        if (typeNames1.size() != typeNames2.size()) {
            return Integer.compare(typeNames1.size(), typeNames2.size());
        }

        for (int i = 0; i < typeNames1.size(); i++) {
            if (!typeNames1.get(i).equals(typeNames2.get(i))) {
                return typeNames1.get(i).compareTo(typeNames2.get(i));
            }
        }
        return 0;
    }

    /**
     * Adds an abstract method to the output file.
     * @param method The abstract method to add.
     * @param writer The BufferedWriter to write the abstract method.
     * @throws IOException If an I/O error occurs while writing to the output file.
     */
    private void addMethod(Method method, BufferedWriter writer) throws IOException {
        StringBuilder methodSB = getMethodSignature(method);
        String returnValue = getReturnValue(method.getReturnType());

        methodSB.append("{").append(LINE_SEP)
                .append(TAB).append(TAB).append("return ").append(returnValue).append(";").append(LINE_SEP)
                .append(TAB).append("}").append(LINE_SEP);
        writer.write(methodSB.toString());
    }

    /**
     * Returns the default return value for a given return type.
     * @param returnType The return type for which the default value is obtained.
     * @return The default return value for the given return type.
     */
    private static String getReturnValue(Class<?> returnType) {
        if (returnType.isPrimitive()) {
            if (returnType.equals(void.class)) {
                return "";
            } else if (returnType.equals(boolean.class)) {
                return "false";
            } else {
                return "0";
            }
        }
        return "null";
    }

    /**
     * Returns a StringBuilder containing the standard signature of an executable (method or constructor).
     * @param executable The executable for which the signature is obtained.
     * @param executableName The name of the executable.
     * @return A StringBuilder containing the standard signature of the executable.
     */
    private StringBuilder getStandardSignature(Executable executable, String executableName) {
        String accessModifier = getAccessModifier(executable);
        String arguments = getArgumentsWithTypes(executable.getParameterTypes());
        String exceptions = getExceptions(executable.getExceptionTypes());

        return new StringBuilder(TAB + accessModifier + " " + covertToUnicode(executableName) + "(" + arguments + ")" + exceptions);
    }

    /**
     * Returns a StringBuilder containing the constructor signature.
     * @param simpleName The simple name of the class.
     * @param method The constructor for which the signature is obtained.
     * @return A StringBuilder containing the constructor signature.
     */
    private StringBuilder getConstructorSignature(String simpleName, Constructor<?> method) {
        return getStandardSignature(method, simpleName + "Impl");
    }

    /**
     * Returns a StringBuilder containing the method signature.
     * @param method The method for which the signature is obtained.
     * @return A StringBuilder containing the method signature.
     */
    private StringBuilder getMethodSignature(Method method) {
        return getStandardSignature(method, method.getReturnType().getCanonicalName() + " " + method.getName());
    }

    /**
     * Joins strings generated by a function.
     * @param len The length of the strings to join.
     * @param function The function generating strings.
     * @return A string joining the strings generated by the function.
     */

    private String joinWithFunction(int len, Function<Integer, String> function) {
        return IntStream.range(0, len)
                .mapToObj(function::apply)
                .collect(Collectors.joining(", "));
    }

    /**
     * Returns a string representing arguments with their types.
     * @param parameterTypes The types of the arguments.
     * @return A string representing arguments with their types.
     */

    private String getArgumentsWithTypes(Class<?>[] parameterTypes) {
        return joinWithFunction(parameterTypes.length, i -> covertToUnicode(parameterTypes[i].getCanonicalName()) + " " + ARGUMENT + i);
    }

    /**
     * Returns a string representing arguments.
     * @param parameterTypes The types of the arguments.
     * @return A string representing arguments.
     */

    private String getArguments(Class<?>[] parameterTypes) {
        return joinWithFunction(parameterTypes.length, i -> ARGUMENT + i);
    }

    /**
     * Returns a string representing types.
     * @param types The types to represent.
     * @return A string representing types.
     */
    private String getTypes(Class<?>[] types) {
        return joinWithFunction(types.length, i -> covertToUnicode(types[i].getCanonicalName()));
    }

    /**
     * Returns a string representing exception types.
     * @param exceptionTypes The exception types to represent.
     * @return A string representing exception types.
     */
    private String getExceptions(Class<?>[] exceptionTypes) {
        if (exceptionTypes.length == 0) {
            return "";
        }
        return " throws " + getTypes(exceptionTypes);
    }

    /**
     * Returns the access modifier of an executable.
     * @param method The executable for which the access modifier is obtained.
     * @return The access modifier of the executable.
     */
    private String getAccessModifier(Executable method) {
        int mod = method.getModifiers();
        mod &= Modifier.methodModifiers() & ~Modifier.ABSTRACT;
        return Modifier.toString(mod);
    }

    /**
     * File visitor for deleting files and directories.
     */
    final private SimpleFileVisitor<Path> VISITOR_DELETER = new SimpleFileVisitor<>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
        }
    };

    /**
     * Implements the provided interface or abstract class and creates a JAR file.
     * @param token The interface/abstract class to implement.
     * @param jarFile The path to the JAR file to create.
     * @throws ImplerException If implementation fails due to any reason.
     */
    @Override
    public void implementJar(Class<?> token, Path jarFile) throws ImplerException {
        Path parentDir = jarFile.getParent().toAbsolutePath();
        Path tmpDir = null;
        try {
            try {
                tmpDir = Files.createTempDirectory(parentDir, "tmp");
            } catch (IOException e) {
                throw new ImplerException("Error while create temp directory", e);
            }
            implement(token, tmpDir);
            compile(token, tmpDir);
            createJar(token, tmpDir, jarFile);
        } finally {
            try {
                if (tmpDir != null) {
                    Files.walkFileTree(tmpDir, VISITOR_DELETER);
                }
            } catch (IOException e) {
                System.err.println("can't delete tmpDir: " + e.getMessage());
            }
        }
    }

    /**
     * Creates a JAR file containing the implemented class.
     * @param token The interface/abstract class implemented.
     * @param tmpDir The temporary directory containing the implemented class.
     * @param jarFile The path to the JAR file to create.
     * @throws ImplerException If an error occurs while creating the JAR file.
     */
    private void createJar(Class<?> token, Path tmpDir, Path jarFile) throws ImplerException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream jarOutputStream = new JarOutputStream(Files.newOutputStream(jarFile), manifest)) {
            jarOutputStream.putNextEntry(new ZipEntry(token.getPackageName().replace(".", "/") +
                    "/" + token.getSimpleName() + "Impl.class"));
            Files.copy(tmpDir.resolve(token.getPackageName().replace(".", FILE_SEP) +
                    FILE_SEP + token.getSimpleName() + "Impl.class"), jarOutputStream);
        } catch (IOException e) {
            throw new ImplerException("Can't create jar", e);
        }
    }

    /**
     * Compiles the implemented class.
     * @param token The interface/abstract class implemented.
     * @param tmpDir The temporary directory containing the implemented class.
     * @throws ImplerException If an error occurs while compiling the implemented class.
     */
    private void compile(Class<?> token, Path tmpDir) throws ImplerException {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new ImplerException("Could not find java compiler");
        }
        String classpath;
        try {
            classpath = Path.of(token.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        } catch (URISyntaxException e) {
            throw new ImplerException("Can't make classpath", e);
        }
        String tName = replaceInPackageName(token) + FILE_SEP + token.getSimpleName() + "Impl.java";
        String filename = tmpDir.resolve(tName).toString();
        String[] args = {
                filename,
                "-cp", classpath,
                "-encoding", StandardCharsets.UTF_8.name()
        };
        final int exitCode = compiler.run(null, null, null, args);
        if (exitCode != 0) {
            throw new ImplerException("Can't compile, exit code: " + exitCode);
        }
    }


    /**
     * Replaces dots in the package name with the system-dependent file separator.
     * @param token The class token for which the package name is modified.
     * @return The modified package name with dots replaced by the system-dependent file separator.
     */
    private static String replaceInPackageName(Class<?> token) {
        return token.getPackageName().replace(".", FILE_SEP);
    }


    /**
     * Converts a string to Unicode escape sequence.
     * @param str The string to convert.
     * @return The string converted to Unicode escape sequence.
     */
    private String covertToUnicode(String str) {
        return str.chars().mapToObj(c -> c < 128 ? Character.toString(c) : String.format("\\u%04X", c)).collect(Collectors.joining());
    }
}
