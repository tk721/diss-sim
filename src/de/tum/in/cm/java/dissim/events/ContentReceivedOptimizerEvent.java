package de.tum.in.cm.java.dissim.events;

import de.tum.in.cm.java.dissim.Algorithm;
import de.tum.in.cm.java.dissim.SimEvent;
import de.tum.in.cm.java.dissim.Stats;

import java.util.Collection;
import java.util.Collections;

/**
 * Used to simulate the optimal dissemination where each active channel will
 * keep transmitting until all nodes have received the message.
 *
 * @author teemuk
 */
public final class ContentReceivedOptimizerEvent
implements SimEvent {

  /** Unserved clients */
  public static int[] CLIENTS;
  /** Position in the unserved clients array */
  public static int CLIENT_POS;

  private final double time;
  private final int node;
  private final int from;
  private final int channel;
  private final double contentSize;
  private final double channelCapacity;
  private final Algorithm.DisseminationStrategy disseminationStrategy;

  //==============================================================================================//
  // API
  //==============================================================================================//
  public ContentReceivedOptimizerEvent(
      final double time,
      final int node,
      final int from,
      final int channel,
      final double contentSize,
      final double channelCapacity,
      final Algorithm.DisseminationStrategy disseminationStrategy ) {
    this.time = time;
    this.node = node;
    this.from = from;
    this.channel = channel;
    this.contentSize = contentSize;
    this.channelCapacity = channelCapacity;
    this.disseminationStrategy = disseminationStrategy;
  }

  @Override
  public final String toString() {
    return "" + this.time + ": ContentReceivedEvent: node = " + this.node
        + ", channel = " + this.channel;
  }

  public static boolean hasClients() {
    return ( CLIENT_POS < CLIENTS.length );
  }

  public static int nextClient() {
    final int nextClient = CLIENTS[ CLIENT_POS ];
    CLIENT_POS++;
    return nextClient;
  }

  public static int clientsLeft() {
    return CLIENTS.length - CLIENT_POS;
  }

  public static int[] getClients( final int count ) {
    final int[] ret = new int[ count ];
    System.arraycopy( CLIENTS, CLIENT_POS, ret, 0, count );
    CLIENT_POS += count;
    return ret;
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
//    System.out.println( "Client node " + this.node + " received content on "
//        + "channel " + this.channel + "" );

    // Record statistics
    final Stats.Reception stats = new Stats.Reception( this.time, this.from,
        this.node, this.channel );
    Stats.RECEPTIONS.add( stats );

    Stats.CLIENT_RECEPTIONS.add( stats );

    // If sequential dissemination, start the next client transfer on this channel.
    if ( this.disseminationStrategy == Algorithm.DisseminationStrategy.SEQUENTIAL
          && hasClients() ) {
      final int nextClient = nextClient();

      final double nextTime = this.time
          + this.contentSize / this.channelCapacity;
      final ContentReceivedOptimizerEvent nextEvent = new ContentReceivedOptimizerEvent(
          nextTime, nextClient, this.from, this.channel, this.contentSize, this.channelCapacity,
          this.disseminationStrategy );
      return Collections.singleton( nextEvent );
    }

    return Collections.emptyList();
  }
  //==============================================================================================//
}
