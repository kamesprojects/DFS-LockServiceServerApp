package dfs.dfs;

import java.net.*;
import java.util.Enumeration;
import java.util.UUID;

public final class OwnerIdUtil {
    private OwnerIdUtil(){}

    public static String buildOwnerId(int dfsPort) {
        String ip = detectNonLoopbackIPv4();
        String name = UUID.randomUUID().toString().substring(0,8);
        return ip + ":" + dfsPort + ":" + name;
    }

    static String detectNonLoopbackIPv4() {
        try {
            String fallback = null;
            for (java.util.Enumeration<java.net.NetworkInterface> en = java.net.NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
                java.net.NetworkInterface ni = en.nextElement();
                if (!ni.isUp() || ni.isLoopback() || ni.isVirtual()) continue;
                for (java.util.Enumeration<java.net.InetAddress> ads = ni.getInetAddresses(); ads.hasMoreElements();) {
                    java.net.InetAddress a = ads.nextElement();
                    if (a instanceof java.net.Inet4Address && !a.isLoopbackAddress()) {
                        if (a.isSiteLocalAddress()) return a.getHostAddress();
                        if (fallback == null) fallback = a.getHostAddress();
                    }
                }
            }
            if (fallback != null) return fallback;
        } catch (Exception ignored) {}
        return "127.0.0.1";
    }

}
