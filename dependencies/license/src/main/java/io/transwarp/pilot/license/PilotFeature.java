package io.transwarp.pilot.license;

import dnw.srvlic.info.v2.CompLicInfoV2;
import io.transwarp.msl.Feature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jiajie on 17-5-11.
 */
public enum PilotFeature implements Feature {
    PILOT("Pilot", CompLicInfoV2.LicEditionV2.APACHE);

    private String name;
    private CompLicInfoV2.LicEditionV2 leastEdition;
    private List<CompLicInfoV2.LicEditionV2> excludeEditions = new ArrayList<>();

    PilotFeature(String name, CompLicInfoV2.LicEditionV2 leastEdition, CompLicInfoV2.LicEditionV2... excludeEditions) {
        this.name = name;
        this.leastEdition = leastEdition;
        this.excludeEditions = Arrays.asList(excludeEditions);
    }

    @Override
    public CompLicInfoV2.LicEditionV2 getLeastEdition() {
        return leastEdition;
    }

    @Override
    public List<CompLicInfoV2.LicEditionV2> getExcludeEditions() {
        return excludeEditions;
    }

    @Override
    public CompLicInfoV2.CompTypeV2 getCompType() {
        return CompLicInfoV2.CompTypeV2.PILOT;
    }

    @Override
    public String toString() {
        return name;
    }
}

