package io.cimpress.mcp.streams.sqs;

import org.slf4j.Marker;


/**
 * Represents an object which has an informational Marker it should be logged with.
 */
public interface Marked {


  /**
   * @return an informational Marker this object should be logged with.
   */
  Marker getMarker();
}
