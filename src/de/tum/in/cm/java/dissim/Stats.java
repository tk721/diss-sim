package de.tum.in.cm.java.dissim;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author teemuk
 */
public final class Stats {
  private Stats() {}

  public static final List <Reception> RECEPTIONS = new ArrayList<>( 1000 );


  public static final List <Reception> CLIENT_RECEPTIONS
      = new ArrayList<>( 1000 );

  public static final List <LeaderActivation> LEADER_ACTIVATIONS
      = new ArrayList<>( 1000 );

  public static final class Reception {
    public final double time;
    public final int source;
    public final int destination;
    public final int channel;

    public Reception(
        double time,
        int source,
        int destination,
        int channel ) {
      this.time = time;
      this.source = source;
      this.destination = destination;
      this.channel = channel;
    }
  }

  public static final class LeaderActivation {
    public final double time;
    public final int leader;

    public LeaderActivation(
        final double time,
        final int leader ) {
      this.time = time;
      this.leader = leader;
    }
  }

  public static void printLeaderCountOverTime( final PrintStream out ) {
    out.println( "# Leader count vs. time" );

    double curTime = 0.0;
    int count = 0;
    for ( final LeaderActivation a : LEADER_ACTIVATIONS ) {
      if ( a.time - curTime > 0.00000001 ) {
        out.println( curTime + " " + count );
      }
      count++;
      curTime = a.time;
    }
    out.println( curTime + " " + count );
  }

  public static void printClientReceptionsPerChannel( final PrintStream out ) {
    out.println( "# Client receptions per channel" );
    out.println( "# <channel> <count>" );
    printCounts( CLIENT_RECEPTIONS, out );
  }

  public static void printReceptionsPerChannel( final PrintStream out ) {
    out.println( "# Total receptions per channel" );
    out.println( "# <channel> <count>" );
    printCounts( RECEPTIONS, out );
  }

  private static void printCounts(
      final Collection <Reception> receptions,
      final PrintStream out ) {
    int maxChannel = 0;
    for ( final Reception reception : receptions ) {
      if ( reception.channel > maxChannel ) maxChannel = reception.channel;
    }

    final int[] channelCounts = new int[ maxChannel ];
    for ( final Reception reception : receptions ) {
      channelCounts[ reception.channel - 1 ]++;
    }

    for ( int i = 0; i < channelCounts.length; i ++ ) {
      out.println( "" + (i + 1) + " " + channelCounts[ i ] );
    }
  }

  public static void printReceptionsBucketed(
      final PrintStream out,
      final double bucketWidth ) {
    out.println( "# Reception CDF" );
    out.println( "# <time> <fraction received>" );

    final int totalCount = RECEPTIONS.size();
    int count = 0;
    double curTime = RECEPTIONS.get( 0 ).time;
    double curValue = 0.0;
    double boundary = bucketWidth;
    for ( final Stats.Reception stat : RECEPTIONS ) {
      if ( stat.time > boundary ) {
        out.println( "" + boundary + " " + curValue );
        boundary += bucketWidth;
      }

      count++;
      final double fraction = 1.0 * count / totalCount;
      curValue = fraction;
      curTime = stat.time;
    }
    out.println( "" + curTime + " " + curValue );
  }
}
