package io.managed.services.test.k8s;

import java.util.LinkedList;
import java.util.List;

/**
 * Stores and provides currently connected openshift clusters
 */
public class KubeClusterConnectionFactory {
    private static List<KubeClusterResource> kubernetesClusters = new LinkedList<>();

    private KubeClusterConnectionFactory() {
    }

    public static synchronized KubeClusterResource connectToKubeCluster(String kubeconfig) {
        KubeClusterResource cluster = new KubeClusterResource(kubeconfig);
        kubernetesClusters.add(cluster);
        return cluster;
    }

    /**
     * Get current connected clusters
     * @return list of KubeClusterResources
     */
    public static synchronized List<KubeClusterResource> getCurrentConnectedClusters() {
        return kubernetesClusters;
    }

    /**
     * Delete connection from suite to cluster
     * @param cluster
     */
    public static synchronized void disconnectFromCluster(KubeClusterResource cluster) {
        kubernetesClusters.remove(cluster);
    }

    /**
     * Delete all already connected clusters
     */
    public static synchronized void disconnectFromAllClusters() {
        kubernetesClusters.clear();
    }
}
