import de.tum.in.cm.java.dissim.Algorithm;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.*;

/**
 * @author teemuk
 */
public class AlgorithmTests {

  //==============================================================================================//
  // Setup/cleanup
  //==============================================================================================//
  @BeforeClass
  public static void setUpBeforeClass()
  throws Exception {

  }

  @AfterClass
  public static void tearDownAfterClass()
  throws Exception {

  }
  //==============================================================================================//


  //==============================================================================================//
  // Tests
  //==============================================================================================//
  @Test
  public void testSelectFollowersApprox()
  throws Exception {
    final int[] hundredClients = getSequence( 100, 0 );

    int[][] split = Algorithm.selectFollowersApprox( hundredClients, 9, 1 );
    assertEquals( "Incorrect local size.", 10, split[ 1 ].length );
    assertEquals( "Incorrect follower size.", 90, split[ 0 ].length );

    int[] expectedLocal = getSequence( 10, 0 );
    int[] expectedFollowers = getSequence( 90, 10 );

    assertArrayEquals( "Incorrect locals", expectedLocal, split[ 1 ] );
    assertArrayEquals( "Incorrect followers", expectedFollowers, split[ 0 ] );

    final int[] tenClients = getSequence( 10, 0 );

    split = Algorithm.selectFollowersApprox( tenClients, 9, 1 );
    assertEquals( "Incorrect local size.", 1, split[ 1 ].length );
    assertEquals( "Incorrect follower size.", 9, split[ 0 ].length );

    final int[] ninetyClients = getSequence( 90, 0 );
    split = Algorithm.selectFollowersApprox( ninetyClients, 18, 1 );
    assertEquals( "Incorrect local size.", 5, split[ 1 ].length );
    assertEquals( "Incorrect follower size.", 85, split[ 0 ].length );

    // No resources to use for followers
    split = Algorithm.selectFollowersApprox( tenClients, 0, 1 );
    assertEquals( "Incorrect local size.", 10, split[ 1 ].length );
    assertEquals( "Incorrect follower size.", 0, split[ 0 ].length );

    // No clients left over from min local
    split = Algorithm.selectFollowersApprox( tenClients, 10, 10 );
    assertEquals( "Incorrect local size.", 10, split[ 1 ].length );
    assertEquals( "Incorrect follower size.", 0, split[ 0 ].length );

    // Large number of lients
    final int[] thousandClients = getSequence( 1000, 1 );
    split = Algorithm.selectFollowersApprox( thousandClients, 8, 1 );
    System.out.println( "Followers: " + split[ 0 ].length + ", local: "
      + split[ 1 ].length );
  }

  @Test
  public void testBranchingFactor()
  throws Exception {
    // Test constrained by desired factor
    int result = Algorithm.branchingFactor( 2, 10, 10 );
    assertEquals( "Incorrect result.", 2, result );

    // Test constrained by number of clients
    result = Algorithm.branchingFactor( 10, 10, 4 );
    assertEquals( "Incorrect result.", 2, result );

    // Test constrained by number of resources
    result = Algorithm.branchingFactor( 10, 2, 10 );
    assertEquals( "Incorrect result.", 2, result );

  }

  @Test
  public void testDivide()
  throws Exception {
    // Test one division
    final int[] tenItems = getSequence( 10, 0 );
    int[][] result = Algorithm.divide( tenItems, 1 );
    assertEquals( "Incorrect number of divisions", 1, result.length );
    assertEquals( "Incorrect division size", 10, result[ 0 ].length );

    // Test even division
    result = Algorithm.divide( tenItems, 2 );
    assertEquals( "Incorrect number of divisions", 2, result.length );
    assertEquals( "Incorrect division size", 5, result[ 0 ].length );
    assertEquals( "Incorrect division size", 5, result[ 1 ].length );

    // Test non-even division
    result = Algorithm.divide( tenItems, 3 );
    assertEquals( "Incorrect number of divisions", 3, result.length );
    assertEquals( "Incorrect division size", 3, result[ 0 ].length );
    assertEquals( "Incorrect division size", 3, result[ 1 ].length );
    assertEquals( "Incorrect division size", 4, result[ 2 ].length );
  }

  @Test
  public void testCalculateWeights()
  throws Exception {
    final int[][] resources = { {1, 2}, {3} };

    final double[] weights = Algorithm.calculateWeights( resources );
    assertEquals( "Incorrect number of weights", 2, weights.length );
    assertEquals( "Incorrect weight", 0.66666666666, weights[ 0 ], 0.00000001 );
    assertEquals( "Incorrect weight", 0.33333333333, weights[ 1 ], 0.00000001 );
  }


  @Test
  public void testDivideWeighted()
  throws Exception {
    final int[][] resources = { {1, 2}, {3} };
    final int[] clients = getSequence( 300, 1 );

    final double[] weights = Algorithm.calculateWeights( resources );
    final int[][] groups = Algorithm.divideWeighted( clients, weights );

    assertEquals( "Incorrect number of groups", 2, groups.length );
    assertEquals( "Incorrect number of clients", 200, groups[ 0 ].length );
    assertEquals( "Incorrect number of clients", 100, groups[ 1 ].length );
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
  //==============================================================================================//
}
