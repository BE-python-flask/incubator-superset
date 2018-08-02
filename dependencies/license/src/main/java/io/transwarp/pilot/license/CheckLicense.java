package io.transwarp.pilot.license;

import io.transwarp.msl.Licensor;

/**
 * Created by jiajie on 17-5-11.
 */
public class CheckLicense {
    public boolean checkLicense(){
        try {
            Licensor.get(PilotFeature.class).setupLicense();
            if (Licensor.get(PilotFeature.class).getLicense().isCompExpiredCurrent()) {
                throw new RuntimeException("Pilot is not available under current licence");
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
