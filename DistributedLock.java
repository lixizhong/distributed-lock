import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryOneTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于zookeeper的分布式锁实现，线程安全、可重入。
 */
public class DistributedLock {
	
	private static Logger logger = LoggerFactory.getLogger(DistributedLock.class);
	
	private static String namespace = "namespaece";
	private static String url = "url";
	private static int sessiontimeout = 300000;
	private static int retryonetime = 50;
	private static int acquiretime = 50;
	
	private static ThreadLocal<Map<String, InterProcessMutex>> mutexThreads = new ThreadLocal<Map<String,InterProcessMutex>>(){
		public Map<String, InterProcessMutex> initialValue(){
			return new HashMap<String, InterProcessMutex>();
		}
	};
	
	private final String lockName;
	
	public DistributedLock(String lockName){
		if(StringUtils.isBlank(lockName)){
			throw new IllegalArgumentException("锁名称不能为空");
		}
		
		if( ! StringUtils.startsWith(lockName, "/")){
			lockName = "/" + lockName;
		}
		
		this.lockName = lockName;
	}
	
	public boolean tryLock(){
		InterProcessMutex mutex = mutexThreads.get().get(lockName);
		if(mutex == null){
			mutex = new InterProcessMutex(getCuratorFrameworkClient(), lockName);
			mutexThreads.get().put(lockName, mutex);
		}
		
		boolean isLock = false;
		
		try {
			isLock = mutex.acquire(acquiretime, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if(isLock){
			logger.info("线程:{},成功获取锁{}", Thread.currentThread().getName(), lockName);
		}else{
			logger.info("线程:{},未得到锁{}", Thread.currentThread().getName(), lockName);
		}
		
		return isLock;
	}
	
	public void release(){
		InterProcessMutex mutex = mutexThreads.get().get(lockName);
		if(mutex == null){
			throw new RuntimeException("线程"+Thread.currentThread().getName()+"没有获得锁");
		}
		try {
			mutex.release();
			logger.info("线程:{},释放锁{}", Thread.currentThread().getName(), lockName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if( ! mutex.isAcquiredInThisProcess()){
			mutexThreads.get().remove(lockName);
			logger.info("线程:{},删除锁对象{}", Thread.currentThread().getName(), lockName);
		}
		
		//解决锁节点不能删除的问题，
		//不用处理异常（异常发生的情况：A、B同时获取锁，A得到锁后，A释放锁，但是还未来得及调用下面的删除代码.
		//此时B得到锁，A执行删除代码，由于此时B在使用锁，删除操作就会抛出异常）
		try {
			getCuratorFrameworkClient().delete().guaranteed().forPath(lockName);
		} catch (Exception e) {
		}
	}
	
	private static CuratorFramework getCuratorFrameworkClient(){
		return CuratorFrameworkClientHolder.INSTANCE;
	}
	
	private static class CuratorFrameworkClientHolder {    
        private static final CuratorFramework INSTANCE = CuratorFrameworkFactory
				.builder()
				.namespace(namespace)
				.connectString(url)
				.sessionTimeoutMs(sessiontimeout)
				.connectionTimeoutMs(retryonetime)
				.retryPolicy(new RetryOneTime(acquiretime))
				.build();
        static{
        	INSTANCE.start();
        	logger.info("DistributedLock: CuratorFramework Client start.");
        }
	}    
}
