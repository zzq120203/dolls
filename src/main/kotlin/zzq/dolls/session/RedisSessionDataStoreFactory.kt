package zzq.dolls.session

import zzq.dolls.redis.RedisPool
import org.eclipse.jetty.server.session.AbstractSessionDataStoreFactory
import org.eclipse.jetty.server.session.SessionDataStore
import org.eclipse.jetty.server.session.SessionHandler


class RedisSessionDataStoreFactory(private var proxy: RedisPool): AbstractSessionDataStoreFactory() {

    @Throws(Exception::class)
    override fun getSessionDataStore(handler: SessionHandler): SessionDataStore {
        val store = RedisSessionDataStore().apply {
            rpp = proxy
        }
        store.gracePeriodSec = gracePeriodSec
        store.savePeriodSec = savePeriodSec
        return store
    }

}