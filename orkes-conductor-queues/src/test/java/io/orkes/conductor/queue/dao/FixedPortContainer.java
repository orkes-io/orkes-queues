package io.orkes.conductor.queue.dao;

import lombok.NonNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.utility.DockerImageName;

public class FixedPortContainer extends GenericContainer {

    public FixedPortContainer(@NonNull DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    public void exposePort(int localPort, int containerPort) {
        super.addFixedExposedPort(localPort, containerPort);
    }

}
