package com.study.nacos;


import com.alibaba.cloud.nacos.registry.NacosRegistration;
import com.alibaba.cloud.nacos.registry.NacosServiceRegistry;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NacosManualService {

	@Resource
	private NacosServiceRegistry nacosServiceRegistry;

	@Resource
	private NacosRegistration nacosRegistration;

	@Resource
	private ApplicationContext applicationContext;

	/**
	 * 手动上线：注册到 Nacos，并允许接收流量
	 *
	 * http://127.0.0.1:8890/online
	 */
	public synchronized void online() {
		AvailabilityChangeEvent.publish(applicationContext, ReadinessState.REFUSING_TRAFFIC);

		log.info("[NACOS] registering... serviceId={}, cluster={}, {}:{}",
				nacosRegistration.getServiceId(),
				nacosRegistration.getCluster(),
				nacosRegistration.getHost(),
				nacosRegistration.getPort());

		nacosServiceRegistry.register(nacosRegistration);

		// 注册成功后再接流量（避免“能被发现但我还没准备好”的窗口）
		AvailabilityChangeEvent.publish(applicationContext, ReadinessState.ACCEPTING_TRAFFIC);
		log.info("[NACOS] online done.");
	}

	/**
	 * 手动下线：拒绝新流量 -> 等待短暂 drain -> 从 Nacos 反注册
	 */
	public synchronized void offline() {

		// 先拒绝流量（配合 k8s readiness / 网关探活，尽量先把新流量切走）
		AvailabilityChangeEvent.publish(applicationContext, ReadinessState.REFUSING_TRAFFIC);


		log.info("[NACOS] deregistering... serviceId={}, cluster={}, {}:{}",
				nacosRegistration.getServiceId(),
				nacosRegistration.getCluster(),
				nacosRegistration.getHost(),
				nacosRegistration.getPort());

		nacosServiceRegistry.deregister(nacosRegistration);

		log.info("[NACOS] offline done.");
	}

}
