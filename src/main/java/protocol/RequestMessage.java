package protocol;

public sealed interface RequestMessage
    permits ApiVersionsRequest, DescribeTopicPartitionsRequest {

}
