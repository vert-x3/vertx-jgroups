package io.vertx.java.spi.cluster.impl.jgroups.support;

public class ComparedValue<T> {

  private final T value;
  private final Enum<ComparedType> compare;

  public ComparedValue(T value, Enum<ComparedType> compare) {
    this.value = value;
    this.compare = compare;
  }

  public T getValue() {
    return value;
  }

  public boolean isEqual() {
    return compare == ComparedType.EQUAL;
  }

  public boolean isLeft() {
    return compare == ComparedType.LEFT;
  }

  public boolean isRight() {
    return compare == ComparedType.RIGHT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ComparedValue)) {
      return false;
    }

    ComparedValue<?> that = (ComparedValue<?>) o;

    return value.equals(that.value) && compare.equals(that.compare);

  }

  @Override
  public int hashCode() {
    int result = value.hashCode();
    result = 31 * result + compare.hashCode();
    return result;
  }
}
