namespace KafkaDataExport.Api.Models;

public sealed class ExportRequest
{
    public required string CustomerId { get; set; }
    public string Format { get; set; } = "json";
    public string RequestedBy { get; set; } = "anonymous";
}

public sealed class ExportRequestMessage
{
    public required string JobId { get; set; }
    public required string CustomerId { get; set; }
    public string Format { get; set; } = "json";
    public string RequestedBy { get; set; } = "anonymous";
    public DateTime RequestedAtUtc { get; set; }
}

public sealed class ExportRequestResponse
{
    public required string JobId { get; set; }
    public required string Status { get; set; }
}

public sealed class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string ExportRequestTopic { get; set; } = "export-requests";
}
