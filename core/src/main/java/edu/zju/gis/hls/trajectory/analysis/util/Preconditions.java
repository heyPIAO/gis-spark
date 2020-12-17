package edu.zju.gis.hls.trajectory.analysis.util;

public final class Preconditions {
    private Preconditions() {
    }

    public static <T> T checkNotNull(T t) {
        return checkNotNull(t, (String)null);
    }

    public static <T> T checkNotNull(T t, String message) {
        if (t == null) {
            throw new NullPointerException(message);
        } else {
            return t;
        }
    }

    public static void checkArgument(boolean b, String message) {
        if (!b) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void checkArgument(boolean b) {
        if (!b) {
            throw new IllegalArgumentException();
        }
    }
}
