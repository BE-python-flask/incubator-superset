import io.transwarp.pilot.license.CheckLicense;
import junit.framework.TestCase;

/**
 * Created by jiajie on 17-5-11.
 */
public class TestLicense extends TestCase {

    public void testCheckLicense(){
        CheckLicense check = new CheckLicense();
        boolean success = check.checkLicense();
        assert success;
    }
}
