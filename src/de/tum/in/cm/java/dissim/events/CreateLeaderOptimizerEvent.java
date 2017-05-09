package de.tum.in.cm.java.dissim.events;

import de.tum.in.cm.java.dissim.Algorithm;
import de.tum.in.cm.java.dissim.Main;
import de.tum.in.cm.java.dissim.SimEvent;
import de.tum.in.cm.java.dissim.Stats;

import java.util.ArrayList;
import java.util.Collection;

/**
 * This event corresponds to the Node.Distribute() algorithm.
 *
 * @author teemuk
 */
public final class CreateLeaderOptimizerEvent
implements SimEvent {

  private final double time;
  private final int channel;
  private final int node;
  private final double channelCapacity;
  private final int[] additionalChannels;
  private final Algorithm.DisseminationStrategy disseminationStrategy;
  private final int[] channelClientCounts;
  private final Main.Args args;

  private final double transmitTime;

  //==============================================================================================//
  // API
  //==============================================================================================//
  /**
   * Creates a new leader event with sequential dissemination strategy.
   *
   * @param time
   * @param node
   * @param channel
   * @param channelCapacity
   * @param additionalChannels
   * @param args
   */
  public CreateLeaderOptimizerEvent(
      final double time,
      final int node,
      final int channel,
      final double channelCapacity,
      final int[] additionalChannels,
      final Main.Args args ) {
    this.time = time;
    this.node = node;
    this.channel = channel;
    this.channelCapacity = channelCapacity;
    this.additionalChannels = additionalChannels;
    this.args = args;

    this.disseminationStrategy = Algorithm.DisseminationStrategy.SEQUENTIAL;
    this.channelClientCounts = null;

    this.transmitTime = args.contentSize / channelCapacity;
  }

  /**
   * Creates a new leader event with concurrent dissemination strategy. The number of clients for
   * each channel must be given. When the event is processed, {@code ContentReceivedOptimizerEvents}
   * are created for all the clients that are configured for this channel.
   *
   * @param time
   * @param node
   * @param channel
   * @param channelCapacity
   * @param additionalChannels
   * @param args
   * @param channelClientCounts
   */
  public CreateLeaderOptimizerEvent(
      final double time,
      final int node,
      final int channel,
      final double channelCapacity,
      final int[] additionalChannels,
      final Main.Args args,
      final int[] channelClientCounts ) {
    this.time = time;
    this.node = node;
    this.channel = channel;
    this.channelCapacity = channelCapacity;
    this.additionalChannels = additionalChannels;
    this.args = args;
    this.channelClientCounts = channelClientCounts;

    this.disseminationStrategy = Algorithm.DisseminationStrategy.CONCURRENT;

    this.transmitTime = args.contentSize / channelCapacity;
  }

  @Override
  public final String toString() {
    return "" + this.time + ": CreateLeaderEvent: node = " + this.node
        + ", channel = " + this.channel;
  }
  //==============================================================================================//


  //==============================================================================================//
  // SimEvent
  //==============================================================================================//
  @Override
  public final double time() {
    return this.time;
  }

  @Override
  public final Collection <SimEvent> process() {
//    System.out.println( "Node " + this.node + " started as leader on channel "
//        + this.channel + "." );

    Stats.LEADER_ACTIVATIONS.add(
        new Stats.LeaderActivation( this.time, this.node ) );

    final Collection <SimEvent> nextEvents = new ArrayList<>();

    final int branchingFactor = Math.min( this.args.branchingFactor,
        this.additionalChannels.length );

    // Recursive step
    if ( branchingFactor > 0 ) {
      final int[][] channelSets
          = Algorithm.divide( this.additionalChannels, branchingFactor );

      for ( int i = 0; i < branchingFactor; i++ ) {
        final int[] channelSet = channelSets[ i ];

        // Pick next leader and channel (leader = channel)
        final int nextLeader = channelSet[ 0 ];
        final int nextChannel = channelSet[ 0 ];

        // Pop next leader and channel from the sets
        final int[] nextChannels = new int[ channelSet.length - 1 ];
        System.arraycopy( channelSet, 1, nextChannels, 0,
            nextChannels.length );

        // Calculate the time when the transfer to this leader will complete
        final double eventTime = this.time + ( ( i + 1 ) * this.transmitTime );

        // Content transfer to the next leader
        final ContentReceivedEvent receivedEvent = new ContentReceivedEvent(
            eventTime, nextLeader, this.node, this.channel );
        nextEvents.add( receivedEvent );

        // Recursive call
        final CreateLeaderOptimizerEvent recursiveEvent;
        if ( this.disseminationStrategy == Algorithm.DisseminationStrategy.SEQUENTIAL ) {
          recursiveEvent = new CreateLeaderOptimizerEvent(
              eventTime + this.args.activationDelay, nextLeader, nextChannel,
              this.channelCapacity, nextChannels, this.args );
        } else if ( this.disseminationStrategy == Algorithm.DisseminationStrategy.CONCURRENT ){
          recursiveEvent = new CreateLeaderOptimizerEvent(
              eventTime + this.args.activationDelay, nextLeader, nextChannel,
              this.channelCapacity, nextChannels, this.args, this.channelClientCounts );
        } else {
          recursiveEvent = null;
          System.err.println( "Unknown dissemination strategy (" + this.disseminationStrategy
              + ")" );
          System.exit( 1 );
        }
        nextEvents.add( recursiveEvent );
      }
    }

    // Local clients
    if ( this.disseminationStrategy == Algorithm.DisseminationStrategy.SEQUENTIAL ) {
      if ( ContentReceivedOptimizerEvent.hasClients() ) {
        final int client = ContentReceivedOptimizerEvent.nextClient();

        final double eventTime = this.time + branchingFactor * this.transmitTime
            + this.transmitTime;
        final ContentReceivedOptimizerEvent receivedEvent
            = new ContentReceivedOptimizerEvent( eventTime, client, this.node,
            this.channel, this.args.contentSize, this.channelCapacity, this.disseminationStrategy );
        nextEvents.add( receivedEvent );
      }
    } else if ( this.disseminationStrategy == Algorithm.DisseminationStrategy.CONCURRENT ) {
      final int clientCount = this.channelClientCounts[ this.channel ];
      // Time it takes to serve all the clients = current time + time to instantiate leaders +
      // time to serve all the clients. The transmissions will all finish at the same time.
      final double eventTime = this.time + branchingFactor * this.transmitTime
          + clientCount * this.transmitTime;
      final int[] clients = ContentReceivedOptimizerEvent.getClients( clientCount );
      for ( final int client : clients ) {
        final ContentReceivedOptimizerEvent receivedEvent
            = new ContentReceivedOptimizerEvent( eventTime, client, this.node,
            this.channel, this.args.contentSize, this.channelCapacity, this.disseminationStrategy );
        nextEvents.add( receivedEvent );
      }
    } else {
      System.err.println( "Unknown dissemination strategy (" + this.disseminationStrategy + ")" );
      System.exit( 1 );
    }

    // Return events
    return nextEvents;
  }
  //==============================================================================================//
}
