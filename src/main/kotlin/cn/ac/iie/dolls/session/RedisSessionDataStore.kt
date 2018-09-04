package cn.ac.iie.dolls.session

import cn.ac.iie.dolls.redis.RPoolProxy
import org.eclipse.jetty.server.session.AbstractSessionDataStore
import org.eclipse.jetty.server.session.SessionData
import org.eclipse.jetty.util.annotation.ManagedAttribute
import org.eclipse.jetty.util.log.Log
import java.io.*
import java.util.concurrent.atomic.AtomicReference
import java.util.HashSet
import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream


class RedisSessionDataStore(private var rpp: RPoolProxy) : AbstractSessionDataStore() {

    private val log = Log.getLogger(RedisSessionDataStore::class.java)

    internal class ObjectInputStreamWithLoader @Throws(IOException::class, StreamCorruptedException::class)
    constructor(`in`: InputStream, private val loader: ClassLoader?) : ObjectInputStream(`in`) {

        init {
            if (loader == null) {
                throw IllegalArgumentException("Illegal null argument to ObjectInputStreamWithLoader")
            }
        }

        override fun resolveClass(classDesc: ObjectStreamClass): Class<*> {
            return Class.forName(classDesc.name, false, this.loader)
        }
    }

    override fun doStart() {
        rpp.init()
        super.doStart()
    }

    override fun doStop() {
        rpp.quit()
        super.doStop()
    }

    @ManagedAttribute(value = "does this store serialize sessions", readonly = true)
    override fun isPassivating(): Boolean {
        return true
    }

    override fun load(id: String): SessionData? {
        val reference = AtomicReference<SessionData>()
        val exception = AtomicReference<Exception>()

        val load = Runnable {
            try {
                log.debug("Loading session {} from Redis", id)
                val session = rpp.jedis { r -> r.get(getCacheKey(id)) }
                session?.byteInputStream()?.use { byteArrayInputStream ->
                    ObjectInputStreamWithLoader(byteArrayInputStream, _context.context.classLoader).use { objectInputStream ->
                        val sd = objectInputStream.readObject() as SessionData
                        reference.set(sd)
                    }
                }
            } catch (e: Exception) {
                exception.set(e)
            }
        }

        //ensure the load runs in the context classloader scope
        _context.run(load)

        if (exception.get() != null)
            throw exception.get()

        return reference.get()
    }

    override fun delete(id: String): Boolean {
        log.debug("Deleting session with id {} from Redis", id)
        return rpp.jedis { r -> r.del(getCacheKey(id)) != null }
    }

    override fun doGetExpired(candidates: MutableSet<String>?): MutableSet<String>? {
        if (candidates == null || candidates.isEmpty())
            return candidates

        val now = System.currentTimeMillis()

        val expired = HashSet<String>()

        candidates.forEach { candidate ->
            log.debug("Checking expiry for candidate {}", candidate)
            try {
                val sd = load(candidate)

                //if the session no longer exists
                if (sd == null) {
                    expired.add(candidate)
                    log.debug("Session {} does not exist in Redis", candidate)
                } else {
                    if (_context.workerName == sd.lastNode) {
                        //we are its manager, add it to the expired set if it is expired now
                        if (sd.expiry in 1..now) {
                            expired.add(candidate)
                            log.debug("Session {} managed by {} is expired", candidate, _context.workerName)
                        }
                    } else {
                        //if we are not the session's manager, only expire it iff:
                        // this is our first expiryCheck and the session expired a long time ago
                        //or
                        //the session expired at least one graceperiod ago
                        if (_lastExpiryCheckTime <= 0) {
                            if (sd.expiry > 0 && sd.expiry < now - 1000L * (3 * _gracePeriodSec))
                                expired.add(candidate)
                        } else {
                            if (sd.expiry > 0 && sd.expiry < now - 1000L * _gracePeriodSec)
                                expired.add(candidate)
                        }
                    }
                }
            } catch (e: Exception) {
                log.warn("Error checking if candidate {} is expired", candidate, e)
            }
        }

        return expired
    }

    override fun doStore(id: String, data: SessionData, lastSaveTime: Long) {
        ByteArrayOutputStream().use { byteArrayOutputStream ->
            ObjectOutputStream(byteArrayOutputStream).use { objectOutputStream ->
                objectOutputStream.writeObject(data)
                rpp.jedis { r -> r.set(getCacheKey(id), byteArrayOutputStream.toString()) }
                log.debug("Session {} saved to Redis, expires {} ", id, data.expiry)
            }
        }
    }

    override fun exists(id: String): Boolean {
        val reference = AtomicReference<Boolean>()
        val exception = AtomicReference<Exception>()

        val load = {
            try {
                val exists = rpp.jedis { r -> r.exists(getCacheKey(id)) }
                if (!exists) {
                    reference.set(false)
                } else {
                    val sd = load(id)
                    if (sd!!.expiry <= 0)
                        reference.set(true)
                    else
                        reference.set(sd.expiry > System.currentTimeMillis())
                }
            } catch (e: Exception) {
                exception.set(e)
            }
        }

        //ensure the load runs in the context classloader scope
        _context.run(load)

        if (exception.get() != null)
            throw exception.get()

        return reference.get()
    }

    private fun getCacheKey(id: String): String {
        return _context.canonicalContextPath + "_" + _context.vhost + "_" + id
    }

}
