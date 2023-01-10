package com.qcloud.chdfs.fs;


import com.tencent.jungle.lb2.L5API;
import com.tencent.jungle.lb2.L5APIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class L5EndpointResolver{
    private static final Logger log = LoggerFactory.getLogger(L5EndpointResolver.class);

    private int modId;
    private int cmdId;

    public L5EndpointResolver(int modId, int cmdId) {
        this.modId = modId;
        this.cmdId = cmdId;
    }

    public String resolveGeneralApiEndpoint() {
        float timeout = 0.2F;
        String cgiIpAddr = null;
        L5API.L5QOSPacket packet = new L5API.L5QOSPacket();
        packet.modid = this.modId;
        packet.cmdid = this.cmdId;

        int maxRetry = 5;
        for (int retryIndex = 1; retryIndex <= maxRetry; ++retryIndex) {
            try {
                packet = L5API.getRoute(packet, timeout);
                if (!packet.ip.isEmpty() && packet.port > 0) {
                    cgiIpAddr = String.format("%s:%d", packet.ip, packet.port);
                    break;
                }
            } catch (L5APIException e) {
                if (retryIndex != maxRetry) {
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextLong(1000L, 3000L));
                    } catch (InterruptedException var) {
                    }
                } else {
                    log.error("Get l5 modid: {} cmdid: {} failed: ", this.modId, this.cmdId, e);
                }
            }
        }

        return cgiIpAddr;
    }
}
