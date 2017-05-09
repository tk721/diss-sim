import de.tum.in.cm.java.dissim.Algorithm;
import de.tum.in.cm.java.dissim.Main;
import de.tum.in.cm.java.dissim.SimEvent;
import de.tum.in.cm.java.dissim.events.CreateLeaderEvent;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collection;

import static org.junit.Assert.*;

/**
 * @author teemuk
 */
public class CreateLeaderEventTests {

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
  public void testProcess()
  throws Exception {
    final double eventTime = 10.0;
    final double channelCapacity = 5.0;
    final int[] clients = getSequence( 20, 1 );
    final int[] channels = getSequence( 4, 1 );
    final double[] channelCaps = { 1.0, 1.0, 1.0, 1.0 };

    final CreateLeaderEvent event = new CreateLeaderEvent( eventTime,
        0, 1, channelCapacity, clients, channels, channelCaps, new Main.Args( new String[0] ) );

    final Collection <SimEvent> events = event.process();

    for ( final SimEvent e : events ) {
      System.out.println( e.toString() );
    }

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
