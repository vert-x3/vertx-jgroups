package io.vertx.spi.cluster.jgroups.domain;

import java.io.*;
import java.util.Collections;
import java.util.Iterator;

public class EmptyChoosableArrayList<T> implements ChoosableArrayList<T> {

  public EmptyChoosableArrayList() {
  }

  @Override
  public ChoosableArrayList<T> add(T value) {
    return new ChoosableArrayListImpl<T>().add(value);
  }

  @Override
  public ChoosableArrayList<T> remove(T value) {
    return this;
  }

  @Override
  public T first() {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public T choose() {
    return null;
  }

  @Override
  public Iterator<T> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
  }

  @Override
  public void writeTo(DataOutput out) throws Exception {
  }

  @Override
  public void readFrom(DataInput in) throws Exception {
  }
}
