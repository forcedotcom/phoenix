/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.hbase.index.covered.update;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import com.google.common.collect.Iterators;

/**
 * A collection whose elements are stored and returned sorted.
 * <p>
 * We can't just use something like a {@link PriorityQueue} because it doesn't return the
 * underlying values in sorted order.
 * @param <T>
 */
class SortedCollection<T> implements Collection<T>, Iterable<T> {

  private final PriorityQueue<T> queue;
  private Comparator<T> comparator;

  /**
   * Use the given comparator to compare all keys for sorting
   * @param comparator
   */
  public SortedCollection(Comparator<T> comparator) {
    this.queue = new PriorityQueue<T>(1, comparator);
    this.comparator = comparator;
  }
  
  /**
   * All passed elements are expected to be {@link Comparable}
   */
  public SortedCollection() {
    this.queue = new PriorityQueue<T>();
  }
  
  @Override
  public int size() {
    return this.queue.size();
  }

  @Override
  public boolean isEmpty() {
    return this.queue.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return this.queue.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    @SuppressWarnings("unchecked")
    T[] array = (T[]) this.queue.toArray();
    if (this.comparator == null) {
      Arrays.sort(array);
    } else {
      Arrays.sort(
     array, this.comparator);}
    return Iterators.forArray(array);
  }

  @Override
  public Object[] toArray() {
    return this.queue.toArray();
  }

  @SuppressWarnings("hiding")
  @Override
  public <T> T[] toArray(T[] a) {
    return this.queue.toArray(a);
  }

  @Override
  public boolean add(T e) {
    return this.queue.add(e);
  }

  @Override
  public boolean remove(Object o) {
    return this.queue.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return this.queue.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    return this.queue.addAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return queue.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return this.queue.retainAll(c);
  }

  @Override
  public void clear() {
    this.queue.clear();
  }
}