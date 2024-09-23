package info.kgeorgiy.ja.denisov.arrayset;


import java.util.*;

public class ArraySet<E extends Comparable<E>> extends AbstractSet<E> implements NavigableSet<E> { // NOTE: comparable

    private final List<E> list;

    private final Comparator<? super E> comparator;

    public ArraySet() {
        this(new ArrayList<>(), null);
    }

    public ArraySet(Comparator<? super E> comparator) {
        this(new ArrayList<>(), comparator);
    }

    public ArraySet(Collection<? extends E> collection) {
        this(collection, null);
    }

    public ArraySet(Collection<? extends E> collection, Comparator<? super E> comparator) {
        TreeSet<E> treeSet = new TreeSet<>(comparator);
        treeSet.addAll(collection);
        this.list = new ArrayList<>(treeSet);
        this.comparator = comparator;
    }

    public ArraySet(List<E> collection, Comparator<? super E> comparator, int l, int r) {
        this.list = collection.subList(l, r);
        this.comparator = comparator;
    }

    private int lowerIndex(E e) {
        return realBinarySearch(e, false, false);
    }

    private int floorIndex(E e) {
        return realBinarySearch(e, false, true);
    }

    private int ceilingIndex(E e) {
        return realBinarySearch(e, true, true);
    }

    private int higherIndex(E e) {
        return realBinarySearch(e, true, false);
    }

    @Override
    public E lower(E e) {
        return get(lowerIndex(e));
    }

    @Override
    public E floor(E e) {
        return get(floorIndex(e));
    }

    @Override
    public E ceiling(E e) {
        return get(ceilingIndex(e));
    }

    @Override
    public E higher(E e) {
        return get(higherIndex(e));
    }

    @Override
    public E pollFirst() {
        throw new UnsupportedOperationException("ArraySet is immutable");
    }

    @Override
    public E pollLast() {
        throw new UnsupportedOperationException("ArraySet is immutable");
    }

    @Override
    public Iterator<E> iterator() {
        return Collections.unmodifiableList(list).iterator();
    }

    @Override
    public NavigableSet<E> descendingSet() {
        return new ArraySet<>(list, Collections.reverseOrder(comparator));
    } // NOTE: not O(1)

    @Override
    public Iterator<E> descendingIterator() {
        return descendingSet().iterator();
    }

    @Override
    public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
        if (compare(fromElement, toElement) > 0) {
            throw new IllegalArgumentException("fromElement is bigger than toElement");
        }

        int l = fromInclusive ? ceilingIndex(fromElement) : higherIndex(fromElement);
        int r = toInclusive ? floorIndex(toElement) : lowerIndex(toElement);
        if (l > r) {
            return new ArraySet<E>(comparator);
        }
        return new ArraySet<>(list, comparator, l, r + 1);
    }

    @Override
    public NavigableSet<E> headSet(E toElement, boolean inclusive) {
        if (size() == 0 || compare(first(), toElement) > 0) {
            return new ArraySet<E>(comparator);
        }
        return subSet(first(), true, toElement, inclusive);
    }

    @Override
    public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
        if (size() == 0 || compare(last(), fromElement) < 0) {
            return new ArraySet<E>(comparator);
        }
        return subSet(fromElement, inclusive, last(), true);
    }

    @Override
    public Comparator<? super E> comparator() {
        return comparator;
    }

    @Override
    public SortedSet<E> subSet(E fromElement, E toElement) {
        return subSet(fromElement, true, toElement, false);
    }

    @Override
    public SortedSet<E> headSet(E toElement) {
        return headSet(toElement, false);
    }

    @Override
    public SortedSet<E> tailSet(E fromElement) {
        return tailSet(fromElement, true);
    }

    @Override
    public E first() {
        if (size() == 0) {
            throw new NoSuchElementException("ArraySet is empty");
        }
        return list.get(0);
    }

    @Override
    public E last() {
        if (size() == 0) {
            throw new NoSuchElementException("ArraySet is empty");
        }
        return list.get(size() - 1);
    }

    @Override
    public int size() {
        return list.size();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        return Collections.binarySearch(list, (E) o, comparator) >= 0;
    }

    public void clear() {
        throw new UnsupportedOperationException("ArraySet is immutable");
    }

    private int compare(E a, E b) {
        if (comparator == null) {
            return a.compareTo(b);
        }
        return comparator.compare(a, b);
    }

    private E get(int index) {
        if (0 <= index && index < size()) {
            return list.get(index);
        }
        return null;
    }

    private int realBinarySearch(E element, boolean bigger, boolean equals) {
        int index = Collections.binarySearch(list, element, comparator);
        if (index >= 0) {
            if (equals) {
                return index;
            }
            index += bigger ? 1 : -1;
            return index;
        }
        index = - index - 2;
        if (bigger) {
            index++;
        }
        return index;
    }
}
