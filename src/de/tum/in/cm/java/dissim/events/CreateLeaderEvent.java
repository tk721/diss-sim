package de.tum.in.cm.java.dissim.events;

import de.tum.in.cm.java.dissim.Algorithm;
import de.tum.in.cm.java.dissim.Main;
import de.tum.in.cm.java.dissim.SimEvent;
import de.tum.in.cm.java.dissim.Stats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This event corresponds to the Node.Distribute() algorithm.
 *
 * @author teemuk
 */
public final class CreateLeaderEvent
implements SimEvent {

  private final double time;
  private final int channel;
  private final int node;
  private final double channelCapacity;
  private final int[] clients;
  private final int[] additionalChannels;
  private final double[] additionalChannelCapacities;
  private final Main.Args args;

  private final double transmitTime;

  //==============================================================================================//
  // API
  //==============================================================================================//
  public CreateLeaderEvent(
      final double time,
      final int node,
      final int channel,
      final double channelCapacity,
      final int[] clients,
      final int[] additionalChannels,
      final double[] additionalChannelCapacities,
      final Main.Args args ) {
    this.time = time;
    this.node = node;
    this.channel = channel;
    this.channelCapacity = channelCapacity;
    this.clients = clients;
    this.additionalChannels = additionalChannels;
    this.additionalChannelCapacities = additionalChannelCapacities;
    this.args = args;

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
//      + this.channel + ". Additional channels: "
//        + this.additionalChannels.length + ", clients: "
//        + this.clients.length );

    Stats.LEADER_ACTIVATIONS.add(
        new Stats.LeaderActivation( this.time, this.node ) );

    final Collection <SimEvent> nextEvents = new ArrayList<>();

    final int[][] clientSplit = this.getLocalSplit();
    final int[] followers = clientSplit[ 0 ];
    int[] localClients = clientSplit[ 1 ];

//    System.out.println( "  local clients: " + localClients.length );

    final int branchingFactor = Algorithm.branchingFactor(
        this.args.branchingFactor, this.additionalChannels.length,
        followers.length );

    // Recursive step
    if ( branchingFactor > 0 ) {
      // Divide the resources into sets
      final int[][] channelSets
          = Algorithm.divide( this.additionalChannels, branchingFactor );
      final double[][] channelCapSets
          = Algorithm.divide( this.additionalChannelCapacities, branchingFactor );

      // Divide the followers into sets
      final int[][] nodeSets;
      if ( this.args.followerDivisionStrategy == Algorithm.FollowerDivisionStrategy.NAIVE ) {
        nodeSets = Algorithm.divide( followers, branchingFactor );
      } else if ( this.args.followerDivisionStrategy
                  == Algorithm.FollowerDivisionStrategy.COUNT_WEIGHTED ) {
        final double[] divisionWeights = Algorithm.calculateWeights( channelSets );
        nodeSets = Algorithm.divideWeighted( followers, divisionWeights );
      } else {
        nodeSets = new int[0][0];
        System.err.println( "Unknown follower division strategy ("
            + this.args.followerDivisionStrategy + ")" );
        System.exit( 1 );
      }

      // Recursive step for every follower/resource set
      for ( int i = 0; i < branchingFactor; i++ ) {
        final int[] channelSet = channelSets[ i ];
        final int[] nodeSet = nodeSets[ i ];
        final double[] channelCapsSet = channelCapSets[ i ];

        // Pick leaders from the local client set
        final int nextLeader = localClients[ i ];
        final int nextChannel = channelSet[ 0 ];
        final double nextCapacity = channelCapsSet[ 0 ];

        // Follower set is just the full node set
        final int[] nextFollowers = nodeSet;
        // Pop the next channel from the sets
        final int[] nextChannels = new int[ channelSet.length - 1 ];
        System.arraycopy( channelSet, 1, nextChannels, 0, nextChannels.length );
        final double[] nextChannelCaps = new double[ channelCapsSet.length - 1 ];
        System.arraycopy( channelCapsSet, 1, nextChannelCaps, 0, nextChannelCaps.length );

        // Calculate the time when this leader will be activated
        final double eventTime = this.time + ( ( i + 1 ) * this.transmitTime );

        // Content transfer to the next leader
        final ContentReceivedEvent receivedEvent = new ContentReceivedEvent(
            eventTime, nextLeader, this.node, this.channel );
        nextEvents.add( receivedEvent );

        // Recursive call
        final CreateLeaderEvent recursiveEvent = new CreateLeaderEvent(
            eventTime + this.args.activationDelay, nextLeader, nextChannel, nextCapacity,
            nextFollowers, nextChannels, nextChannelCaps, this.args );
        nextEvents.add( recursiveEvent );
      }
    } else {
      if ( followers.length > 0 ) {
        System.err.println( "Branching factor zero, while followers are not zero." );
        // Since we're not branching, we need to serve all clients directly.
        localClients = this.clients;
        System.exit( 0 );
      }
    }

    // Local clients. I.e., the clients that were not picked as leaders for
    // the recursive calls.
    final double clientStartTime
        = this.time + branchingFactor * this.transmitTime;
    if ( this.args.dissStrategy == Algorithm.DisseminationStrategy.SEQUENTIAL ) {
      nextEvents.addAll( this.sequentialDissemination( clientStartTime, localClients,
          branchingFactor ) );
    } else if ( this.args.dissStrategy == Algorithm.DisseminationStrategy.CONCURRENT ) {
      nextEvents.addAll( this.concurrentDissemination( clientStartTime, localClients,
          branchingFactor ) );
    } else {
      throw new RuntimeException( "Invalid dissemination strategy" );
    }

    // Return events
    return nextEvents;
  }
  //==============================================================================================//

  //==============================================================================================//
  // Private
  //==============================================================================================//
  private int[][] getLocalSplit() {
    if ( this.args.localSplitStrategy == Algorithm.LocalSplitStrategy.NAIVE ) {
      return Algorithm.selectFollowersApprox( this.clients, this.additionalChannels.length,
          this.args.branchingFactor /* leaders come from local clients */ );
    } else if ( this.args.localSplitStrategy
        == Algorithm.LocalSplitStrategy.SWITCH_DELAY_CORRECTED ) {
      return Algorithm.selectFollowersWithSwitchDelay(
          this.clients, this.additionalChannels.length,
          this.args.branchingFactor /* leaders come from local clients */,
          this.args.activationDelay, this.transmitTime );
    } else {
      System.err.println( "Unknown local split strategy (" + this.args.localSplitStrategy + ")" );
      System.exit( 1 );
    }
    return new int[0][0];
  }

  private List <SimEvent> sequentialDissemination(
      final double startTime,
      final int[] localClients,
      final int firstClient ) {
    final List <SimEvent> events
        = new ArrayList<>( localClients.length - firstClient );
    for ( int i = firstClient; i < localClients.length; i++ ) {
      final int client = localClients[ i ];
      final double eventTime = startTime + ( ( i + 1 ) * this.transmitTime );
      final ContentReceivedEvent receivedEvent = new ContentReceivedEvent(
          eventTime, client, this.node, this.channel );
      events.add( receivedEvent );
    }
    return events;
  }

  private List <SimEvent> concurrentDissemination(
      final double startTime,
      final int[] localClients,
      final int firstClient  ) {
    final double finishTime = startTime
        + localClients.length * this.args.contentSize / this.channelCapacity;
    final List <SimEvent> events
        = new ArrayList<>( localClients.length - firstClient );
    for ( int i = firstClient; i < localClients.length; i++ ) {
      final int client = localClients[ i ];
      final double eventTime = finishTime;
      final ContentReceivedEvent receivedEvent = new ContentReceivedEvent(
          eventTime, client, this.node, this.channel );
      events.add( receivedEvent );
    }
    return events;
  }
  //==============================================================================================//


}
