package de.tum.in.cm.java.dissim.events;

import de.tum.in.cm.java.dissim.SimEvent;
import de.tum.in.cm.java.dissim.Stats;

import java.util.Collection;
import java.util.Collections;

/**
 * @author teemuk
 */
public final class ContentReceivedEvent
implements SimEvent {

  private final double time;
  private final int node;
  private final int from;
  private final int channel;

  //==============================================================================================//
  // API
  //==============================================================================================//
  public ContentReceivedEvent(
      final double time,
      final int node,
      final int from,
      final int channel ) {
    this.time = time;
    this.node = node;
    this.from = from;
    this.channel = channel;
  }

  @Override
  public final String toString() {
    return "" + this.time + ": ContentReceivedEvent: node = " + this.node
        + ", channel = " + this.channel;
  }
  //==============================================================================================//

  //==============================================================================================//
  // SimEvent
  //==============================================================================================//
  @Override
  public double time() {
    return this.time;
  }

  @Override
  public Collection <SimEvent> process() {
//    System.out.println( "" + this.time + ": Client node " + this.node + " "
//        + "received content on " + "channel " + this.channel + "" );

    final Stats.Reception stats = new Stats.Reception( this.time, this.from,
        this.node, this.channel );
    Stats.RECEPTIONS.add( stats );

    return Collections.emptyList();
  }
  //==============================================================================================//
}
