package cn.ac.iie.dolls.session

import cn.ac.iie.dolls.redis.RPoolProxy
import org.eclipse.jetty.server.session.AbstractSessionDataStoreFactory
import org.eclipse.jetty.server.session.SessionDataStore
import org.eclipse.jetty.server.session.SessionHandler


class RedisSessionDataStoreFactory(private val rpp: RPoolProxy): AbstractSessionDataStoreFactory() {

    @Throws(Exception::class)
    override fun getSessionDataStore(handler: SessionHandler): SessionDataStore {
        val store = RedisSessionDataStore(rpp)
        store.gracePeriodSec = gracePeriodSec
        store.savePeriodSec = savePeriodSec
        return store
    }

}