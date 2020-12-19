package com.zzq.dolls.redis.module.json;

/**
 * Path is a ReJSON path, representing a valid path into an object
 */
public class Path {

    public static final com.redislabs.modules.rejson.Path ROOT_PATH = new com.redislabs.modules.rejson.Path(".");

    private final String strPath;

    public Path(final String strPath) {
        this.strPath = strPath;
    }

    /**
     * Makes a root path
     * @return the root path
     * @deprecated use {@link #ROOT_PATH} instead
     */
    @Deprecated
    public static com.redislabs.modules.rejson.Path RootPath() {
        return new com.redislabs.modules.rejson.Path(".");
    }

    @Override
    public String toString() {
        return strPath;
    }
}
