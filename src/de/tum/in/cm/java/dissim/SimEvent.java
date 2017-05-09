package de.tum.in.cm.java.dissim;

import java.util.Collection;

/**
 * @author teemuk
 */
public interface SimEvent {
  /**
   * Time instance for this event.
   *
   * @return
   *    The time instance for this event.
   */
  double time();

  /**
   * Processes this event. Will be called by the simulator when the
   * simulation time reaches the time instance for this event.
   *
   * @return
   *    Collection of events created by the processing of this event.
   */
  Collection <SimEvent> process();
}
