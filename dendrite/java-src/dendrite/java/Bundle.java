/**
 * Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
 *
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 * which can be found in the file epl-v10.txt at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 *
 * You must not remove this notice, or any other, from this software.
 */

package dendrite.java;

import clojure.lang.ArrayChunk;
import clojure.lang.IChunk;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class Bundle implements Iterable<List> {

  public final List[] columnValues;
  private final boolean[] isColumnRepeated;
  private final int maxBundleSize;
  private final long firstRecordIndex;

  Bundle(int maxBundleSize, long firstRecordIndex, boolean[] isColumnRepeated, List[] columnValues) {
    this.isColumnRepeated = isColumnRepeated;
    this.columnValues = columnValues;
    this.maxBundleSize = maxBundleSize;
    this.firstRecordIndex = firstRecordIndex;
  }

  public int size() {
    return columnValues[0].size();
  }

  @Override
  public Iterator<List> iterator() {
    return Arrays.asList(columnValues).iterator();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof Bundle)) {
      return false;
    } else {
      Bundle b = (Bundle)o;
      if (columnValues.length != b.columnValues.length) {
        return false;
      } else {
        for (int i=0; i<columnValues.length; ++i) {
          List l = columnValues[i];
          List ol = b.columnValues[i];
          if (!l.equals(ol)) {
            return false;
          }
        }
        return true;
      }
    }
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  private ListIterator[] getColumnIterators() {
    ListIterator[] columnIterators = new ListIterator[columnValues.length];
    for (int i=0; i<columnValues.length; ++i) {
      if (isColumnRepeated[i]) {
        columnIterators[i] = new RepeatedValuesIterator(columnValues[i]);
      } else {
        columnIterators[i] = columnValues[i].listIterator();
      }
    }
    return columnIterators;
  }

  public IChunk assemble(Assemble.Fn assemblyFn) {
    ListIterator[] columnIterators = getColumnIterators();
    Object[] assembledRecords = new Object[maxBundleSize];
    for (int i=0; i<maxBundleSize; ++i) {
      assembledRecords[i] = assemblyFn.invoke(columnIterators);
    }
    return new ArrayChunk(assembledRecords);
  }

  private void skip(ListIterator[] columnIterators) {
    int n = columnIterators.length;
    for (int i=0; i<n; ++i) {
      if (isColumnRepeated[i]) {
        ((RepeatedValuesIterator)columnIterators[i]).skip();
      } else {
        columnIterators[i].next();
      }
    }
  }

  public IChunk assembleSampled(Assemble.Fn assemblyFn, IFn sampleFn) {
    ListIterator[] columnIterators = getColumnIterators();
    Object[] assembledRecords = new Object[maxBundleSize];
    int n = 0;
    long recordIndex = firstRecordIndex;
    for (int i=0; i<maxBundleSize; ++i) {
      if (RT.booleanCast(sampleFn.invoke(recordIndex))) {
        assembledRecords[n] = assemblyFn.invoke(columnIterators);
        n += 1;
      } else {
        skip(columnIterators);
      }
      recordIndex += 1;
    }
    return new ArrayChunk(assembledRecords, 0, n);
  }

  public IChunk assembleFiltered(Assemble.Fn assemblyFn, IFn filterFn) {
    ListIterator[] columnIterators = getColumnIterators();
    Object[] assembledRecords = new Object[maxBundleSize];
    int n = 0;
    for (int i=0; i<maxBundleSize; ++i) {
      Object o = assemblyFn.invoke(columnIterators);
      if (RT.booleanCast(filterFn.invoke(o))) {
        assembledRecords[n] = o;
        n += 1;
      }
    }
    return new ArrayChunk(assembledRecords, 0, n);
  }

  public IChunk assembleSampledAndFiltered(Assemble.Fn assemblyFn, IFn sampleFn, IFn filterFn) {
    ListIterator[] columnIterators = getColumnIterators();
    Object[] assembledRecords = new Object[maxBundleSize];
    int n = 0;
    long recordIndex = firstRecordIndex;
    for (int i=0; i<maxBundleSize; ++i) {
      if (RT.booleanCast(sampleFn.invoke(recordIndex))) {
        Object o = assemblyFn.invoke(columnIterators);
        if (RT.booleanCast(filterFn.invoke(o))) {
          assembledRecords[n] = o;
          n += 1;
        }
      } else {
        skip(columnIterators);
      }
      recordIndex += 1;
    }
    return new ArrayChunk(assembledRecords, 0, n);
  }

  public Object reduce(IFn reduceFn, Assemble.Fn assemblyFn, Object init) {
    Object ret = init;
    ListIterator[] columnIterators = getColumnIterators();
    for (int i=0; i<maxBundleSize; ++i) {
      ret = reduceFn.invoke(ret, assemblyFn.invoke(columnIterators));
    }
    return ret;
  }

  public Object reduceSampled(IFn reduceFn, Assemble.Fn assemblyFn, IFn sampleFn, Object init) {
    Object ret = init;
    ListIterator[] columnIterators = getColumnIterators();
    long recordIndex = firstRecordIndex;
    for (int i=0; i<maxBundleSize; ++i) {
      if (RT.booleanCast(sampleFn.invoke(recordIndex))) {
        ret = reduceFn.invoke(ret, assemblyFn.invoke(columnIterators));
      } else {
        skip(columnIterators);
      }
      recordIndex += 1;
    }
    return ret;
  }

  public Object reduceFiltered(IFn reduceFn, Assemble.Fn assemblyFn, IFn filterFn, Object init) {
    Object ret = init;
    ListIterator[] columnIterators = getColumnIterators();
    for (int i=0; i<maxBundleSize; ++i) {
      Object o = assemblyFn.invoke(columnIterators);
      if (RT.booleanCast(filterFn.invoke(o))) {
        ret = reduceFn.invoke(ret, o);
      }
    }
    return ret;
  }

  public Object reduceSampledAndFiltered(IFn reduceFn, Assemble.Fn assemblyFn, IFn sampleFn,
                                         IFn filterFn, Object init) {
    Object ret = init;
    ListIterator[] columnIterators = getColumnIterators();
    long recordIndex = firstRecordIndex;
    for (int i=0; i<maxBundleSize; ++i) {
      if (RT.booleanCast(sampleFn.invoke(recordIndex))) {
        Object o = assemblyFn.invoke(columnIterators);
        if (RT.booleanCast(filterFn.invoke(o))) {
          ret = reduceFn.invoke(ret, o);
        }
      } else {
        skip(columnIterators);
      }
      recordIndex += 1;
    }
    return ret;
  }

  public Bundle take(int n) {
    List[] takenColumnValues = new List[columnValues.length];
    for (int i=0; i<columnValues.length; ++i) {
      takenColumnValues[i] = columnValues[i].subList(0, n);
    }
    return new Bundle(n, firstRecordIndex, isColumnRepeated, takenColumnValues);
  }

  public Bundle drop(int n) {
    List[] remainingColumnValues = new List[columnValues.length];
    for (int i=0; i<columnValues.length; ++i) {
      List values = columnValues[i];
      remainingColumnValues[i] = values.subList(n, values.size());
    }
    return new Bundle(maxBundleSize - n, firstRecordIndex, isColumnRepeated, remainingColumnValues);
  }

  public static final class Factory {

    private final boolean[] isColumnRepeated;
    private final int numColumns;
    private long nextFirstRecordIndex;

    public Factory(Schema.Column[] columns) {
      this.numColumns = columns.length;
      this.isColumnRepeated = new boolean[numColumns];
      this.nextFirstRecordIndex = 0;
      for (int i=0; i<columns.length; ++i) {
        isColumnRepeated[i] = (columns[i].repetitionLevel > 0);
      }
    }

    public Bundle create(int bundleSize, List[] columnValues) {
      Bundle b = new Bundle(bundleSize, nextFirstRecordIndex, isColumnRepeated, columnValues);
      nextFirstRecordIndex += bundleSize;
      return b;
    }

    @SuppressWarnings("unchecked")
    public Bundle stripe(Stripe.Fn stripeFn, List<Object> records) {
      List[] columnValues = new List[numColumns];
      int bundleSize = records.size();
      for (int i=0; i<numColumns; ++i) {
        columnValues[i] = new ArrayList(bundleSize);
      }
      Object[] buffer = new Object[numColumns];
      boolean success = false;
      for (Object record : records) {
        Arrays.fill(buffer, null);
        success = stripeFn.invoke(record, buffer);
        if (success) {
          for (int i=0; i<numColumns; ++i) {
            columnValues[i].add(buffer[i]);
          }
        }
      }
      return create(bundleSize, columnValues);
    }
  }

  static final class RepeatedValuesIterator implements ListIterator<Object> {

    final Iterator<List<Object>> listIterator;
    Iterator<Object> currentIterator;
    Object previousValue;
    boolean isPreviousCalled;

    RepeatedValuesIterator(List<List<Object>> repeatedValues) {
      this.listIterator = repeatedValues.iterator();
      currentIterator = listIterator.next().iterator();
      this.previousValue = null;
      this.isPreviousCalled = false;
    }

    @Override
    public void add(Object o) {
      throw new UnsupportedOperationException();
    }

    void skip() {
      if (isPreviousCalled) {
        isPreviousCalled = false;
        step();
      } else if (currentIterator != null) {
        if (currentIterator.hasNext()) {
          step();
        } else {
          step();
          skip();
        }
      }
    }

    void step() {
      if (listIterator.hasNext()) {
        currentIterator = listIterator.next().iterator();
      } else {
        currentIterator = null;
      }
    }

    @Override
    public boolean hasNext() {
      if (isPreviousCalled) {
        return true;
      } else if (currentIterator == null) {
        return false;
      } else if (currentIterator.hasNext()) {
        return true;
      } else {
        step();
        return hasNext();
      }
    }

    @Override
    public boolean hasPrevious() {
      return isPreviousCalled;
    }

    @Override
    public Object next() {
      if (isPreviousCalled) {
        isPreviousCalled = false;
        return previousValue;
      } else if (currentIterator == null) {
        throw new NoSuchElementException();
      } else if (!currentIterator.hasNext()) {
        step();
        return next();
      } else {
        previousValue = currentIterator.next();
        return previousValue;
      }
    }

    @Override
    public Object previous() {
      isPreviousCalled = true;
      return previousValue;
    }

    @Override
    public int nextIndex() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int previousIndex() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void set(Object o) {
      throw new UnsupportedOperationException();
    }
  }
}
