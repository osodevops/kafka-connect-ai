package sh.oso.connect.ai.adapter.grpc;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.grpc.config.GrpcConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.util.BoundedRecordBuffer;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GrpcSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(GrpcSourceAdapter.class);

    private ManagedChannel channel;
    private GrpcConfig config;
    private Descriptors.MethodDescriptor protoMethodDescriptor;
    private MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethodDescriptor;
    private BoundedRecordBuffer buffer;
    private long lastPollTimestamp;
    private final AtomicBoolean streamingActive = new AtomicBoolean(false);

    @Override
    public String type() {
        return "grpc";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.GRPC_TARGET, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "gRPC target in host:port format")
                .define(AiConnectConfig.GRPC_SERVICE, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "Fully qualified gRPC service name")
                .define(AiConnectConfig.GRPC_METHOD, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "gRPC method name to invoke")
                .define(AiConnectConfig.GRPC_MODE, ConfigDef.Type.STRING,
                        "unary", ConfigDef.Importance.MEDIUM,
                        "gRPC invocation mode: unary or server-streaming")
                .define(AiConnectConfig.GRPC_TLS_ENABLED, ConfigDef.Type.BOOLEAN,
                        false, ConfigDef.Importance.MEDIUM,
                        "Enable TLS for gRPC connection")
                .define(AiConnectConfig.GRPC_PROTO_DESCRIPTOR_PATH, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Path to compiled .desc protobuf descriptor file")
                .define(AiConnectConfig.GRPC_POLL_INTERVAL_MS, ConfigDef.Type.LONG,
                        5000L, ConfigDef.Importance.MEDIUM,
                        "Poll interval in ms for unary mode");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new GrpcConfig(props);
        this.buffer = new BoundedRecordBuffer();
        this.lastPollTimestamp = 0;

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                .forTarget(config.target());

        if (!config.tlsEnabled()) {
            channelBuilder.usePlaintext();
        }

        this.channel = channelBuilder.build();

        resolveMethodDescriptors();

        if ("server-streaming".equals(config.mode())) {
            startServerStreaming();
        }

        log.info("GrpcSourceAdapter started: target={}, service={}, method={}, mode={}",
                config.target(), config.service(), config.method(), config.mode());
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        if ("server-streaming".equals(config.mode())) {
            return buffer.drain(maxRecords, Duration.ofMillis(config.pollIntervalMs()));
        }

        // Unary polling mode
        long now = System.currentTimeMillis();
        long elapsed = now - lastPollTimestamp;
        if (elapsed < config.pollIntervalMs()) {
            Thread.sleep(config.pollIntervalMs() - elapsed);
        }
        lastPollTimestamp = System.currentTimeMillis();

        try {
            DynamicMessage request = DynamicMessage.getDefaultInstance(
                    protoMethodDescriptor.getInputType());

            DynamicMessage response = ClientCalls.blockingUnaryCall(
                    channel,
                    grpcMethodDescriptor,
                    CallOptions.DEFAULT,
                    request);

            String jsonResponse = JsonFormat.printer()
                    .omittingInsignificantWhitespace()
                    .print(response);

            RawRecord record = new RawRecord(
                    null,
                    jsonResponse.getBytes(StandardCharsets.UTF_8),
                    Map.of(
                            "grpc.service", config.service(),
                            "grpc.method", config.method()
                    ),
                    SourceOffset.empty()
            );

            return List.of(record);
        } catch (Exception e) {
            throw new RetryableException("Failed to invoke gRPC unary call: " +
                    config.service() + "/" + config.method(), e);
        }
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        // gRPC source does not maintain offsets
    }

    @Override
    public boolean isHealthy() {
        return channel != null && !channel.isShutdown();
    }

    @Override
    public void stop() {
        streamingActive.set(false);

        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (buffer != null) {
            buffer.clear();
        }

        log.info("GrpcSourceAdapter stopped");
    }

    private void resolveMethodDescriptors() {
        try {
            String descriptorPath = config.protoDescriptorPath();
            if (descriptorPath == null || descriptorPath.isEmpty()) {
                throw new NonRetryableException(
                        "Proto descriptor path must be set via " + AiConnectConfig.GRPC_PROTO_DESCRIPTOR_PATH);
            }

            DescriptorProtos.FileDescriptorSet descriptorSet;
            try (FileInputStream fis = new FileInputStream(descriptorPath)) {
                descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(fis);
            }

            Descriptors.FileDescriptor[] fileDescriptors = new Descriptors.FileDescriptor[descriptorSet.getFileCount()];
            for (int i = 0; i < descriptorSet.getFileCount(); i++) {
                fileDescriptors[i] = Descriptors.FileDescriptor.buildFrom(
                        descriptorSet.getFile(i),
                        i > 0 ? new Descriptors.FileDescriptor[]{fileDescriptors[i - 1]}
                                : new Descriptors.FileDescriptor[0]);
            }

            Descriptors.ServiceDescriptor serviceDescriptor = null;
            for (Descriptors.FileDescriptor fd : fileDescriptors) {
                serviceDescriptor = fd.findServiceByName(extractSimpleName(config.service()));
                if (serviceDescriptor != null) {
                    break;
                }
            }

            if (serviceDescriptor == null) {
                throw new NonRetryableException("Service not found in descriptor: " + config.service());
            }

            protoMethodDescriptor = serviceDescriptor.findMethodByName(config.method());
            if (protoMethodDescriptor == null) {
                throw new NonRetryableException(
                        "Method not found: " + config.method() + " in service " + config.service());
            }

            MethodDescriptor.MethodType methodType = protoMethodDescriptor.isServerStreaming()
                    ? MethodDescriptor.MethodType.SERVER_STREAMING
                    : MethodDescriptor.MethodType.UNARY;

            String fullMethodName = MethodDescriptor.generateFullMethodName(
                    config.service(), config.method());

            grpcMethodDescriptor = MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                    .setType(methodType)
                    .setFullMethodName(fullMethodName)
                    .setRequestMarshaller(ProtoUtils.marshaller(
                            DynamicMessage.getDefaultInstance(protoMethodDescriptor.getInputType())))
                    .setResponseMarshaller(ProtoUtils.marshaller(
                            DynamicMessage.getDefaultInstance(protoMethodDescriptor.getOutputType())))
                    .build();

        } catch (NonRetryableException e) {
            throw e;
        } catch (Exception e) {
            throw new NonRetryableException("Failed to resolve gRPC method descriptors", e);
        }
    }

    private void startServerStreaming() {
        streamingActive.set(true);

        DynamicMessage request = DynamicMessage.getDefaultInstance(
                protoMethodDescriptor.getInputType());

        ClientCall<DynamicMessage, DynamicMessage> call = channel.newCall(
                grpcMethodDescriptor, CallOptions.DEFAULT);

        ClientCalls.asyncServerStreamingCall(call, request, new StreamObserver<>() {
            @Override
            public void onNext(DynamicMessage message) {
                if (!streamingActive.get()) {
                    return;
                }
                try {
                    String json = JsonFormat.printer()
                            .omittingInsignificantWhitespace()
                            .print(message);

                    RawRecord record = new RawRecord(
                            null,
                            json.getBytes(StandardCharsets.UTF_8),
                            Map.of(
                                    "grpc.service", config.service(),
                                    "grpc.method", config.method(),
                                    "grpc.mode", "server-streaming"
                            ),
                            SourceOffset.empty()
                    );

                    if (!buffer.offer(record)) {
                        log.warn("Buffer full, dropping gRPC streaming message");
                    }
                } catch (Exception e) {
                    log.error("Error processing streaming gRPC message", e);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (streamingActive.get()) {
                    log.error("gRPC server stream error for {}/{}: {}",
                            config.service(), config.method(), t.getMessage());
                }
            }

            @Override
            public void onCompleted() {
                log.info("gRPC server stream completed for {}/{}",
                        config.service(), config.method());
            }
        });
    }

    private static String extractSimpleName(String fullyQualifiedName) {
        int lastDot = fullyQualifiedName.lastIndexOf('.');
        return lastDot >= 0 ? fullyQualifiedName.substring(lastDot + 1) : fullyQualifiedName;
    }
}
