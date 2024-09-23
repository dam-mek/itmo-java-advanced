package info.kgeorgiy.ja.denisov.student;

import info.kgeorgiy.java.advanced.student.Group;
import info.kgeorgiy.java.advanced.student.GroupName;
import info.kgeorgiy.java.advanced.student.GroupQuery;
import info.kgeorgiy.java.advanced.student.Student;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StudentDB implements GroupQuery {
    private static final Comparator<Student> STUDENT_COMPARATOR = Comparator
            .comparing(Student::getLastName)
            .thenComparing(Student::getFirstName)
            .thenComparing(Student::getId, Comparator.reverseOrder());

    private static final Comparator<Student> STUDENT_ID_COMPARATOR = Comparator.comparing(Student::getId);

    private static final Comparator<Map.Entry<GroupName, Long>> GROUP_COMPARATOR_REVERSED =
            Map.Entry.<GroupName, Long>comparingByValue()
                    .thenComparing(Map.Entry.<GroupName, Long>comparingByKey().reversed());

    private static final Comparator<Map.Entry<GroupName, Long>> GROUP_COMPARATOR =
            Map.Entry.<GroupName, Long>comparingByValue()
                    .thenComparing(Map.Entry.comparingByKey());

    private <R> Stream<Map.Entry<GroupName, R>> collectStudentsByGroup(Collection<Student> collection, Collector<Student, ?, R> collector) {
        return collection.stream()
                .collect(Collectors.groupingBy(Student::getGroup, collector))
                .entrySet().stream();
    }


    private List<Group> getGroupsWith(Collection<Student> collection, Comparator<Student> studentComparator) {
        return collectStudentsByGroup(collection, Collectors.toList())
                .sorted(Map.Entry.comparingByKey()) // :NOTE: reuse comp
                .map(entry -> new Group(
                        entry.getKey(),
                        entry.getValue().stream().sorted(studentComparator).toList()
                )).toList();
    }

    @Override
    public List<Group> getGroupsByName(Collection<Student> collection) {
        return getGroupsWith(collection, STUDENT_COMPARATOR);
    }

    @Override
    public List<Group> getGroupsById(Collection<Student> collection) {
        return getGroupsWith(collection, STUDENT_ID_COMPARATOR);
    }

    private GroupName getLargestGroupWith(Collection<Student> collection,
                                          Collector<Student, ?, Long> studentCollector,
                                          Comparator<Map.Entry<GroupName, Long>> groupComparator) {
        return collectStudentsByGroup(collection, studentCollector)
                .max(groupComparator)
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    @Override
    public GroupName getLargestGroup(Collection<Student> collection) {
        return getLargestGroupWith(
                collection,
                Collectors.counting(),
                GROUP_COMPARATOR
        );
    }

    @Override
    public GroupName getLargestGroupFirstName(Collection<Student> collection) {
        return getLargestGroupWith(
                collection,
                Collectors.collectingAndThen(
                        Collectors.mapping(Student::getFirstName, Collectors.toSet()),
                        set -> (long) set.size()),
                GROUP_COMPARATOR_REVERSED
        );
    }

    private <R> List<R> getStudentsWithMap(List<Student> list, Function<Student, R> mapper) {
        return list.stream().map(mapper).toList();
    }

    @Override
    public List<String> getFirstNames(List<Student> list) {
        return getStudentsWithMap(list, Student::getFirstName);
    }

    @Override
    public List<String> getLastNames(List<Student> list) {
        return getStudentsWithMap(list, Student::getLastName);
    }

    @Override
    public List<GroupName> getGroups(List<Student> list) {
        return getStudentsWithMap(list, Student::getGroup);
    }

    @Override
    public List<String> getFullNames(List<Student> list) {
        return getStudentsWithMap(list, student -> student.getFirstName() + " " + student.getLastName());
    }

    @Override
    public Set<String> getDistinctFirstNames(List<Student> list) {
        return list.stream().map(Student::getFirstName).collect(Collectors.toCollection(TreeSet::new));
    }

    @Override
    public String getMaxStudentFirstName(List<Student> list) {
        return list.stream().max(Student::compareTo).map(Student::getFirstName).orElse("");
    }

    private List<Student> sortStudentBy(Collection<Student> collection, Comparator<Student> comparator) {
        return collection.stream().sorted(comparator).toList();
    }

    @Override
    public List<Student> sortStudentsById(Collection<Student> collection) {
        return sortStudentBy(collection, Student::compareTo);
    }

    @Override
    public List<Student> sortStudentsByName(Collection<Student> collection) {
        return sortStudentBy(collection, STUDENT_COMPARATOR);
    }

    private List<Student> findStudentsByPredicate(Collection<Student> collection,
                                                  Predicate<Student> predicate) {
        return sortStudentsByName(collection).stream().filter(predicate).toList();
    }


    private <T> List<Student> findStudentsByPredicateEqualTo(Collection<Student> collection, Function<Student, T> getFirstName, T s) {
        return findStudentsByPredicate(collection, student -> Objects.equals(getFirstName.apply(student), s));
    }

    @Override
    public List<Student> findStudentsByFirstName(Collection<Student> collection, String s) {
        return findStudentsByPredicateEqualTo(collection, Student::getFirstName, s);
    }

    @Override
    public List<Student> findStudentsByLastName(Collection<Student> collection, String s) {
        return findStudentsByPredicateEqualTo(collection, Student::getLastName, s);
    }

    @Override
    public List<Student> findStudentsByGroup(Collection<Student> collection, GroupName groupName) {
        return findStudentsByPredicateEqualTo(collection, Student::getGroup, groupName);
    }

    @Override
    public Map<String, String> findStudentNamesByGroup(Collection<Student> collection, GroupName groupName) {
        return findStudentsByGroup(collection, groupName).stream()
                .collect(Collectors.toMap(
                        Student::getLastName,
                        Student::getFirstName,
                        BinaryOperator.minBy(Comparator.naturalOrder())
                ));
    }
}
