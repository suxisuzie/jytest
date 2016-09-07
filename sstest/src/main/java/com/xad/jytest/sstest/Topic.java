package com.xad.jytest.sstest;

import com.google.protobuf.ByteString;
import com.xad.enigma.AdDemandPartnerReportingRequestTopic;
import com.xad.enigma.AdRequestTopic;

/**
 * Created by suziesu on 9/7/16.
 */
/*
 * Copyright (c) 2013.  xAd Inc.  All Rights Reserved.
 */
import com.xad.enigma.AdDemandPartnerReportingRequestTopic.AdDemandPartnerReportingRequest;
import com.xad.enigma.AdDetailsTopic.AdDetails;
import com.xad.enigma.AdRequestTopic.AdRequest;
import com.xad.enigma.AdTrackingTopic.AdTracking;
import com.xad.enigma.AdUserProfileTopic.AdUserProfile;
import com.xad.enigma.DelUserProfileTopic.DelUserProfile;
import com.xad.enigma.RTITrackingTopic.RTITracking;
import com.xad.enigma.SampleTrackingTopic.SampleTracking;
import com.xad.enigma.HttpVendorStatsTopic.HttpVendorStats;
import com.xad.enigma.SegmentBuilderTopic.SegmentBuilder;
import com.xad.enigma.UserTrackingTopic.UserTracking;
import com.xad.enigma.BlockAttributesTopic.BlockAttributes;
import com.xad.enigma.AdDocumentTopic.AdDocument;
import com.xad.enigma.RTITrackingTopic.RTITracking;
import com.xad.enigma.AtlanticMetaDataTopic.AtlanticMetaData;
import com.xad.enigma.VisitTrackingTopic.VisitTracking;
import com.xad.enigma.VisitTrackingTopic.AdExposure;
import com.xad.enigma.ControlGroupTopic.ControlGroup;
import com.xad.enigma.AdMarkupTopic.AdMarkup;


enum Topic {
    AD_DEMAND_PARTNER_REPORTING_REQUEST ("enigma.event.AdDemandPartnerReportingRequest", AdDemandPartnerReportingRequest.class),
    AD_REQUEST ("enigma.event.AdRequest",  AdRequest.class ),
    AD_DETAILS ("enigma.event.AdDetails",  AdDetails.class ),
    AD_TRACKING("enigma.event.AdTracking", AdTracking.class),
    AD_USER_PROFILE("enigma.event.AdUserProfile", AdUserProfile.class),
    UPDATE_USER_PROFILE("enigma.event.UpdateUserProfile", AdUserProfile.class),
    DEL_USER_PROFILE("enigma.event.DelUserProfile", DelUserProfile.class),
    SAMPLE_TRACKING("enigma.event.SampleTracking", SampleTracking.class),
    HTTP_VENDOR_STATS("enigma.event.HttpVendorStats", HttpVendorStats.class),
    SEGMENT_BUILDER("enigma.event.SegmentBuilder", SegmentBuilder.class),
    USER_TRACKING("enigma.event.UserTracking", UserTracking.class),
    BLOCK_ATTRIBUTES("enigma.event.BlockAttributes", BlockAttributes.class),
    AD_DOCUMENT("enigma.event.AdDocument", AdDocument.class),
    RTI_TRACKING("enigma.event.RTITracking", RTITracking.class),
    ATLANTIC_METADATA("enigma.event.rev.AtlanticMetaData",AtlanticMetaData.class),
    VISIT_TRACKING("enigma.event.VisitTracking",VisitTracking.class),
    AD_EXPOSURE("enigma.event.AdExposure", AdExposure.class),
    AD_EXPOSURE_STATE("enigma.event.AdExposureState", AdExposure.class),
    CONTROL_GROUP("enigma.event.SvlControlGroup",ControlGroup.class),
    AD_MARKUP("enigma.event.AdMarkup", AdMarkup.class),
    DATA_REQUEST ("enigma.event.DataRequest",  AdRequest.class );

    public String topicName;
    public Class<com.google.protobuf.Message> protoClass;

    Topic(String topicName, Class protoClass){
        this.topicName = topicName;
        this.protoClass = protoClass;
    }

    public static String toTopicName(Topic topic, String trafficStream, String market) {
        String topicName = topic.topicName;
        if (null != trafficStream) {
            topicName += "-" + trafficStream.toLowerCase();
        }

        if (null != market) {
            topicName += "." + market.toLowerCase();
        }
        return topicName;
    }

    public static Topic fromName(String name) {
        for(Topic t: values()) {

            if (name.startsWith(t.topicName)) {
                return t;
            }
        }
        return null;
    }

    //TODO: Need to take trafficStream and market into consideration
    public static boolean isValidTopic(String name) {
        if (null == fromName(name)) {
            return false;
        }
        return true;
    }

    public String simpleName() {
        return protoClass.getSimpleName();
    }
}
