package io.vertx.java.spi.cluster.impl.jgroups.support;

import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class MyLogFormatter extends Formatter {

  private static final MessageFormat messageFormat = new MessageFormat("{3,date,hh:mm:ss};{1};{2};{0};{5};{4}\n");

  public MyLogFormatter() {
    super();
  }

  @Override
  public String format(LogRecord record) {
    Object[] arguments = new Object[6];
    arguments[0] = record.getLoggerName();
    arguments[1] = record.getLevel();
    arguments[2] = Thread.currentThread().getName();
    arguments[3] = new Date(record.getMillis());
    arguments[4] = record.getMessage();
    arguments[5] = record.getSourceMethodName();
    return messageFormat.format(arguments);
  }

}