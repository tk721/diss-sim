package de.tum.in.cm.java.dissim;

import de.tum.in.cm.java.dissim.events.ContentReceivedOptimizerEvent;
import de.tum.in.cm.java.dissim.events.CreateLeaderEvent;
import de.tum.in.cm.java.dissim.events.CreateLeaderOptimizerEvent;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Event based simulator for studying the algorithm from "Practical Opportunistic Content
 * Dissemination Performance in Dense Network Segments".
 *
 * @author teemuk
 */
public final class Main {

  //==============================================================================================//
  // Constants
  //==============================================================================================//
  public static String ARG_CLIENT_COUNT = "clients";
  public static String ARG_CONTENT_SIZE = "contentSize";
  public static String ARG_SPLIT_STRATEGY = "localSplit";
  public static String ARG_MODE = "mode";
  public static String ARG_DIVISION_STRATEGY = "followerDivision";
  public static String ARG_CHANNEL_COUNT = "channels";
  public static String ARG_CHANNEL_CAPACITY = "channelCapacity";
  public static String ARG_ACTIVATION_DELAY = "activationDelay";
  public static String ARG_BRANCHING_FACTOR = "branchingFactor";
  public static String ARG_DISS_STRATEGY = "dissStrategy";
  public static String ARG_CHANNEL_USE_REPORT = "channelReport";
  public static String ARG_LEADER_COUNT_REPORT = "leaderCountReport";
  public static String ARG_BUCKETED_RECEPTION_REPORT = "bucketedReceptionReport";

  private static final Comparator <SimEvent> EVENT_COMPARATOR = ( o1, o2 ) -> {
        if ( o1.time() == o2.time() ) return 0;
        return ( o1.time() < o2.time() ) ? ( -1 ) : ( 1 );
      };

  private static final PriorityQueue <SimEvent> EVENT_QUEUE
      = new PriorityQueue<>( EVENT_COMPARATOR );
  //==============================================================================================//


  //==============================================================================================//
  // Main
  //==============================================================================================//
  public static void main( final String[] argStrings ) throws FileNotFoundException {
    final Args args = new Args( argStrings );

    // Setup the simulation
    if ( args.runMode == RunMode.NORMAL ) {
      System.out.println( "Starting normal run." );
      setupNormal( args );
    } else if ( args.runMode == RunMode.OPTIMIZING ) {
      System.out.println( "Starting optimizing run." );
      setupOptimizer( args );
    }

    // Run the event loop
    final long startTime = System.nanoTime();
    int eventCount = 0;
    while ( EVENT_QUEUE.size() != 0 ) {
      eventCount++;
      final SimEvent event = EVENT_QUEUE.remove();
      final Collection <SimEvent> newEvents = event.process();
      EVENT_QUEUE.addAll( newEvents );
    }
    final long endTime = System.nanoTime();

    final double durationMillis = 1.0 * ( endTime - startTime ) / 1000000.0;
    System.out.println( "Processed " + eventCount + " events in " + durationMillis + "ms." );


    // Output reports
    if ( args.channelReportFile != null ) try ( PrintStream out = new PrintStream(
        args.channelReportFile ) ) {
      Stats.printReceptionsPerChannel( out );
    }

    if ( args.bucketedReceptionReport != null ) try ( PrintStream out = new PrintStream(
        args.bucketedReceptionReport ) ) {
      Stats.printReceptionsBucketed( out, args.receptionReportBucketWidth );
    }

    if ( args.leaderCountReport != null ) try ( PrintStream out = new PrintStream(
        args.leaderCountReport ) ) {
      Stats.printLeaderCountOverTime( out );
    }
  }
  //==============================================================================================//


  //==============================================================================================//
  // Arguments
  //==============================================================================================//
  public static final class Args {
    public static final int DEFAULT_CLIENT_COUNT = 500;
    public static final double DEFAULT_CONTENT_SIZE = 1.0;
    public static final Algorithm.LocalSplitStrategy DEFAULT_SPLIT_STRATEGY
        = Algorithm.LocalSplitStrategy.NAIVE;
    public static final RunMode DEFAULT_RUN_MODE = RunMode.NORMAL;
    public static final Algorithm.FollowerDivisionStrategy DEFAULT_FOLLOWER_DIVISION
        = Algorithm.FollowerDivisionStrategy.NAIVE;
    public static final int DEFAULT_CHANNEL_COUNT = 3;
    public static final double DEFAULT_CHANNEL_CAPACITY = 1.0;
    public static final double DEFAULT_ACTIVATION_DELAY = 0.0;
    public static final int DEFAULT_BRANCHING_FACTOR = 2;
    public static final Algorithm.DisseminationStrategy DEFAULT_DISS_STRATEGY
        = Algorithm.DisseminationStrategy.SEQUENTIAL;
    public static final double DEFAULT_BUCKET_WIDTH = 10.0;

    public final int clientCount;
    public final double contentSize;
    public final Algorithm.LocalSplitStrategy localSplitStrategy;
    public final Algorithm.FollowerDivisionStrategy followerDivisionStrategy;
    public final RunMode runMode;
    public final int channelCount;
    public final double channelCapacity;
    public final double activationDelay;
    public final int branchingFactor;
    public final Algorithm.DisseminationStrategy dissStrategy;
    public final File channelReportFile;
    public final File leaderCountReport;
    public final File bucketedReceptionReport;
    public final double receptionReportBucketWidth;

    public Args( final String[] args ) {
      // Defaults
      Algorithm.LocalSplitStrategy localSplitStrategy = DEFAULT_SPLIT_STRATEGY;
      RunMode runMode = DEFAULT_RUN_MODE;
      Algorithm.FollowerDivisionStrategy followerDivisionStrategy = DEFAULT_FOLLOWER_DIVISION;
      int channelCount = DEFAULT_CHANNEL_COUNT;
      double activationDelay = DEFAULT_ACTIVATION_DELAY;
      int clientCount = DEFAULT_CLIENT_COUNT;
      double contentSize = DEFAULT_CONTENT_SIZE;
      double channelCapacity = DEFAULT_CHANNEL_CAPACITY;
      int branchingFactor = DEFAULT_BRANCHING_FACTOR;
      Algorithm.DisseminationStrategy dissStrategy = DEFAULT_DISS_STRATEGY;
      File channelReportFile = null;
      File leaderCountReport = null;
      File bucketedReceptionReport = null;
      double receptionReportBucketWidth = DEFAULT_BUCKET_WIDTH;

      // Parse args
      for ( int i = 0; i < args.length; i++ ) {
        final String argString = args[ i ];
        if ( argString.startsWith( ARG_CLIENT_COUNT + "=" ) ) {
          final String value = argString.substring( ARG_CLIENT_COUNT.length() + 1 );
          clientCount = Integer.parseInt( value );
        } else if ( argString.startsWith( ARG_CONTENT_SIZE + "=" ) ) {
          final String value = argString.substring( ARG_CONTENT_SIZE.length() + 1 );
          contentSize = Double.parseDouble( value );
        } else if ( argString.startsWith( ARG_SPLIT_STRATEGY + "=" ) ) {
          final String value = argString.substring( ARG_SPLIT_STRATEGY.length() + 1 );
          localSplitStrategy = Algorithm.LocalSplitStrategy.fromString( value );
        } else if ( argString.startsWith( ARG_MODE + "=" ) ) {
          final String value = argString.substring( ARG_MODE.length() + 1 );
          runMode = RunMode.fromString( value );
        } else if ( argString.startsWith( ARG_DIVISION_STRATEGY + "=" ) ) {
          final String value = argString.substring( ARG_DIVISION_STRATEGY.length() + 1 );
          followerDivisionStrategy = Algorithm.FollowerDivisionStrategy.fromString( value );
        } else if ( argString.startsWith( ARG_CHANNEL_COUNT + "=" ) ) {
          final String value = argString.substring( ARG_CHANNEL_COUNT.length() + 1 );
          channelCount = Integer.parseInt( value );
        } else if ( argString.startsWith( ARG_CHANNEL_CAPACITY + "=" ) ) {
          final String value = argString.substring( ARG_CHANNEL_CAPACITY.length() + 1 );
          channelCapacity = Double.parseDouble( value );
        } else if ( argString.startsWith( ARG_ACTIVATION_DELAY + "=" ) ) {
          final String value = argString.substring( ARG_ACTIVATION_DELAY.length() + 1 );
          activationDelay = Double.parseDouble( value );
        } else if ( argString.startsWith( ARG_BRANCHING_FACTOR + "=" ) ) {
          final String value = argString.substring( ARG_BRANCHING_FACTOR.length() + 1 );
          branchingFactor = Integer.parseInt( value );
        } else if ( argString.startsWith( ARG_DISS_STRATEGY + "=" ) ) {
          final String value = argString.substring( ARG_DISS_STRATEGY.length() + 1 );
          dissStrategy = Algorithm.DisseminationStrategy.fromString( value );
        } else if ( argString.startsWith( ARG_CHANNEL_USE_REPORT + "=" ) ) {
          final String value = argString.substring( ARG_CHANNEL_USE_REPORT.length() + 1 );
          channelReportFile = new File( value );
        } else if ( argString.startsWith( ARG_LEADER_COUNT_REPORT + "=" ) ) {
          final String value = argString.substring( ARG_LEADER_COUNT_REPORT.length() + 1 );
          leaderCountReport = new File( value );
        } else if ( argString.startsWith( ARG_BUCKETED_RECEPTION_REPORT + "=" ) ) {
          final String value = argString.substring( ARG_BUCKETED_RECEPTION_REPORT.length() + 1 );
          final String[] split = value.split( ";" );
          receptionReportBucketWidth = Double.parseDouble( split[ 0 ] );
          bucketedReceptionReport = new File( split[ 1 ] );
        } else {
          System.err.println( "Unknown argument '" + argString + "'" );
          System.exit( 1 );
        }
      }

      // Set args
      this.clientCount = clientCount;
      this.contentSize = contentSize;
      this.localSplitStrategy = localSplitStrategy;
      this.runMode = runMode;
      this.followerDivisionStrategy = followerDivisionStrategy;
      this.channelCount = channelCount;
      this.channelCapacity = channelCapacity;
      this.activationDelay = activationDelay;
      this.branchingFactor = branchingFactor;
      this.dissStrategy = dissStrategy;
      this.channelReportFile = channelReportFile;
      this.leaderCountReport = leaderCountReport;
      this.bucketedReceptionReport = bucketedReceptionReport;
      this.receptionReportBucketWidth = receptionReportBucketWidth;
    }
  }
  //==============================================================================================//


  //==============================================================================================//
  private static void setupOptimizer( final Args args ) {
    // Scenario
    final int[] nodes = getSequence( args.clientCount, 2 );
    final int[] channels = getSequence( args.channelCount - 1, 2 );

    // Set up the clients
    ContentReceivedOptimizerEvent.CLIENTS
        = new int[ nodes.length - channels.length ];
    System.arraycopy( nodes, channels.length,
        ContentReceivedOptimizerEvent.CLIENTS, 0,
        ContentReceivedOptimizerEvent.CLIENTS.length );

    // Create initial event
    final CreateLeaderOptimizerEvent initialEvent
        = new CreateLeaderOptimizerEvent( 0.0, 1, 1, args.channelCapacity, channels, args );
//    final CreateLeaderOptimizerEvent initialEvent
//        = new CreateLeaderOptimizerEvent( 0.0, 1, 1, args.contentSize, args.channelCapacity,
//            channels, args, new int[] { 0, 250, 249 } );

    EVENT_QUEUE.add( initialEvent );


  }

  private static void setupNormal( final Args args ) {
    // Scenario
    final int[] nodes = getSequence( args.clientCount, 2 );
    final int[] channels = getSequence( args.channelCount - 1, 2 );
    final double[] channelCaps = getArray( args.channelCount - 1, args.channelCapacity, 0.0 );

    // Create initial event
    final CreateLeaderEvent initialEvent = new CreateLeaderEvent( 0.0, 1, 1,
        args.channelCapacity, nodes, channels, channelCaps, args );

    EVENT_QUEUE.add( initialEvent );
  }
  //==============================================================================================//


  //==============================================================================================//
  // Analysis
  //==============================================================================================//
  private static void analyzeReceptions( final List <Stats.Reception> stats ) {
    System.out.println( "Collected " + stats.size() + " receptions." );

    final int totalCount = stats.size();
    int count = 0;
    for ( final Stats.Reception stat : stats ) {
      count++;
      final double fraction = 1.0 * count / totalCount;
      System.out.println( "" + stat.time + " " + fraction );
    }
  }

  private static void analyzeReceptionsFiltered(
      final List <Stats.Reception> stats ) {
    System.out.println( "Dissemination CDF:" );

    final int totalCount = stats.size();
    int count = 0;
    double curTime = stats.get( 0 ).time;
    double curValue = 0.0;
    for ( final Stats.Reception stat : stats ) {
      if ( stat.time - curTime > 0.0000000001 ) {
        System.out.println( "" + curTime + " " + curValue );
      }

      count++;
      final double fraction = 1.0 * count / totalCount;
      curValue = fraction;
      curTime = stat.time;
    }
    System.out.println( "" + curTime + " " + curValue );
  }


  //==============================================================================================//


  //==============================================================================================//
  // Private
  //==============================================================================================//
  private static int[] getSequence( final int count, final int firstVal ) {
    final int[] vals = new int[ count ];
    for ( int i = 0; i < count; i++ ) {
      vals[ i ] = firstVal + i;
    }
    return vals;
  }

  private static double[] getArray(
      final int count,
      final double initialValue,
      final double delta ) {
    final double[] vals = new double[ count ];
    for ( int i = 0; i < count; i++ ) {
      vals[ i ] = initialValue + i * delta;
    }
    return vals;
  }

  /**
   * Simulation run type. Either normal or optimizing run.
   */
  private enum RunMode {
    NORMAL, OPTIMIZING;

    public final String toString() {
      if ( this == NORMAL ) return "normal";
      else if ( this == OPTIMIZING ) return "optimizing";
      else return "unknown";
    }

    public static RunMode fromString( final String string ) {
      if ( string.equals( NORMAL.toString() ) ) return NORMAL;
      else if ( string.equals( OPTIMIZING.toString() ) )
        return OPTIMIZING;
      else return null;
    }
  }
  //==============================================================================================//
}
