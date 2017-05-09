package de.tum.in.cm.java.dissim;

/**
 * Functions used by the Node.Distribute() algorithm.
 *
 * @author teemuk
 */
public final class Algorithm {
  private Algorithm() {}

  public static int[][] selectFollowersApprox(
      final int[] clients,
      final int numChannels,
      final int minLocal ) {
    if ( clients.length <= minLocal || numChannels == 0 ) {
      return new int[][] { {}, clients };
    }

    // Number of clients per channel
    final double split = 1.0 * clients.length / ( numChannels + 1.0 );

    final int localSize = ( int ) Math.max( Math.ceil( split ), minLocal );
    final int followerSize = clients.length - localSize;

    final int[] localClients = new int[ localSize ];
    System.arraycopy( clients, 0, localClients, 0, localSize );
    final int[] followers = new int[ followerSize ];
    System.arraycopy( clients, localSize, followers, 0, followerSize );

    return new int[][]{ followers, localClients };
  }

  public static int[][] selectFollowersWithSwitchDelay(
      final int[] clients,
      final int numChannels,
      final int minLocal,
      final double switchDelay,
      final double transmissionTime ) {
    if ( clients.length <= minLocal || numChannels == 0 ) {
      return new int[][] { {}, clients };
    }

    // Number of clients per channel
    final double localCount = 1.0 * ( clients.length
        + ( numChannels * switchDelay / transmissionTime ) )
          / ( numChannels + 1.0 );

    final int localSize = ( int ) Math.min(
            Math.max( Math.ceil( localCount ), minLocal ),
            clients.length );
    final int followerSize = clients.length - localSize;

    final int[] localClients = new int[ localSize ];
    System.arraycopy( clients, 0, localClients, 0, localSize );
    final int[] followers = new int[ followerSize ];
    System.arraycopy( clients, localSize, followers, 0, followerSize );

    return new int[][]{ followers, localClients };
  }

  public static int branchingFactor(
      final int desiredFactor,
      final int numChannels,
      final int numClients ) {
    //final int max = Math.min( Math.floorDiv( numClients, 2 ), numChannels );
    final int max = Math.min( numClients / 2, numChannels );
    return Math.min( desiredFactor, max );
  }

  public static int[][] divide(
      final int[] input,
      final int count ) {
    // Precondition
    if ( count < 0 ) {
      throw new IllegalArgumentException( "count must be " + "positive" );
    }
    if ( input.length < count ) {
      throw new IllegalArgumentException( "input array length must be greater"
          + " than count" );
    }

    final int subsetSize = input.length / count;
    final int[][] output = new int[ count ][];
    for ( int i = 0; i < count; i++ ) {
      final int position = i * subsetSize;
      final int size = ( i == count - 1 ) ?
          ( input.length - position ) : ( subsetSize );
      final int[] subset = new int[ size ];
      System.arraycopy( input, position, subset, 0, size );
      output[ i ] = subset;
    }

    return output;
  }

  public static double[][] divide(
      final double[] input,
      final int count ) {
    // Precondition
    if ( count < 0 ) {
      throw new IllegalArgumentException( "count must be " + "positive" );
    }
    if ( input.length < count ) {
      throw new IllegalArgumentException( "input array length must be greater"
          + " than count" );
    }

    final int subsetSize = input.length / count;
    final double[][] output = new double[ count ][];
    for ( int i = 0; i < count; i++ ) {
      final int position = i * subsetSize;
      final int size = ( i == count - 1 ) ?
          ( input.length - position ) : ( subsetSize );
      final double[] subset = new double[ size ];
      System.arraycopy( input, position, subset, 0, size );
      output[ i ] = subset;
    }

    return output;
  }

  public static int[][] divideWeighted(
      final int[] input,
      final double[] weights ) {
    final int count = weights.length;
    final int[][] output = new int[ count ][];

    int pos = 0;
    for ( int i = 0; i < count; i++ ) {
      // Calculate the number of clients in this group
      final int size;
      if ( i == count - 1 ) {
        // Last group, take all the remaining ones
        size = input.length - pos;
      } else {
        size = ( int )( weights[ i ] * input.length );
      }

      final int[] subset = new int[ size ];
      System.arraycopy( input, pos, subset, 0, size );
      output[ i ] = subset;
      pos += size;
    }

    return output;
  }

  public static double[] calculateWeights( final int[][] input ) {
    final int count = input.length;
    final double[] weights = new double[ count ];

    int total = 0;
    for ( int i = 0; i < count; i++ ) {
      total += input[ i ].length;
    }
    for ( int i = 0; i < count; i++ ) {
      weights[ i ] = 1.0 * input[ i ].length / total;
    }
    return weights;
  }

  public enum DisseminationStrategy {
    CONCURRENT, SEQUENTIAL;

    public static DisseminationStrategy fromString( final String string ) {
      if ( string.equals( CONCURRENT.toString() ) ) return CONCURRENT;
      else if ( string.equals( SEQUENTIAL.toString() ) ) return SEQUENTIAL;
      else return null;
    }
  }

  public enum LocalSplitStrategy {
    NAIVE, SWITCH_DELAY_CORRECTED;

    public final String toString() {
      if ( this == NAIVE ) return "naive";
      else if ( this == SWITCH_DELAY_CORRECTED ) return "switch_delay_corrected";
      else return "unknown";
    }

    public static LocalSplitStrategy fromString( final String string ) {
      if ( string.equals( NAIVE.toString() ) ) return NAIVE;
      else if ( string.equals( SWITCH_DELAY_CORRECTED.toString() ) )
        return SWITCH_DELAY_CORRECTED;
      else return null;
    }
  }

  public enum FollowerDivisionStrategy {
    NAIVE, COUNT_WEIGHTED, CAPACITY_WEIGHTED;

    public final String toString() {
      if ( this == NAIVE ) return "naive";
      else if ( this == COUNT_WEIGHTED ) return "count_weighted";
      else if ( this == CAPACITY_WEIGHTED ) return "capacity_weighted";
      else return "unknown";
    }

    public static FollowerDivisionStrategy fromString( final String string ) {
      if ( string.equals( NAIVE.toString() ) ) return NAIVE;
      else if ( string.equals( COUNT_WEIGHTED.toString() ) )
        return COUNT_WEIGHTED;
      else if ( string.equals( CAPACITY_WEIGHTED.toString() ) )
        return CAPACITY_WEIGHTED;
      else return null;
    }
  }
}
