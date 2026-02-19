package sh.oso.connect.ai.adapter.grpc;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.grpc.config.GrpcConfig;
import sh.oso.connect.ai.api.adapter.SinkAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GrpcSinkAdapter implements SinkAdapter {

    private static final Logger log = LoggerFactory.getLogger(GrpcSinkAdapter.class);

    private ManagedChannel channel;
    private GrpcConfig config;
    private Descriptors.MethodDescriptor protoMethodDescriptor;
    private MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethodDescriptor;

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
                .define(AiConnectConfig.GRPC_TLS_ENABLED, ConfigDef.Type.BOOLEAN,
                        false, ConfigDef.Importance.MEDIUM,
                        "Enable TLS for gRPC connection")
                .define(AiConnectConfig.GRPC_PROTO_DESCRIPTOR_PATH, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Path to compiled .desc protobuf descriptor file");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new GrpcConfig(props);

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                .forTarget(config.target());

        if (!config.tlsEnabled()) {
            channelBuilder.usePlaintext();
        }

        this.channel = channelBuilder.build();

        resolveMethodDescriptors();

        log.info("GrpcSinkAdapter started: target={}, service={}, method={}",
                config.target(), config.service(), config.method());
    }

    @Override
    public void write(List<TransformedRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        for (TransformedRecord record : records) {
            try {
                DynamicMessage.Builder requestBuilder = DynamicMessage.newBuilder(
                        protoMethodDescriptor.getInputType());

                if (record.value() != null && record.value().length > 0) {
                    String json = new String(record.value(), StandardCharsets.UTF_8);
                    JsonFormat.parser().ignoringUnknownFields().merge(json, requestBuilder);
                }

                DynamicMessage request = requestBuilder.build();

                ClientCalls.blockingUnaryCall(
                        channel,
                        grpcMethodDescriptor,
                        CallOptions.DEFAULT,
                        request);

                log.debug("Successfully sent record via gRPC to {}/{}",
                        config.service(), config.method());

            } catch (Exception e) {
                throw new RetryableException(
                        "Failed to send record via gRPC to " + config.service() + "/" + config.method(), e);
            }
        }
    }

    @Override
    public void flush() {
        // gRPC sink uses synchronous unary calls; no buffered writes to flush
    }

    @Override
    public boolean isHealthy() {
        return channel != null && !channel.isShutdown();
    }

    @Override
    public void stop() {
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

        log.info("GrpcSinkAdapter stopped");
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

            String fullMethodName = MethodDescriptor.generateFullMethodName(
                    config.service(), config.method());

            grpcMethodDescriptor = MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
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

    private static String extractSimpleName(String fullyQualifiedName) {
        int lastDot = fullyQualifiedName.lastIndexOf('.');
        return lastDot >= 0 ? fullyQualifiedName.substring(lastDot + 1) : fullyQualifiedName;
    }
}
