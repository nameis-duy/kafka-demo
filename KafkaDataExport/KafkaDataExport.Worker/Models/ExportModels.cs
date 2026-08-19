namespace KafkaDataExport.Worker.Models;

public sealed class ExportRequestMessage
{
    public required string JobId { get; set; }
    public required string CustomerId { get; set; }
    public string Format { get; set; } = "json";
    public string RequestedBy { get; set; } = "anonymous";
    public DateTime RequestedAtUtc { get; set; }
}

public sealed class ExportDataMessage
{
    public required string JobId { get; set; }
    public required string CustomerId { get; set; }
    public string Format { get; set; } = "json";
    public string RequestedBy { get; set; } = "anonymous";
    public DateTime GeneratedAtUtc { get; set; }
    public int TotalRows { get; set; }
    public List<CustomerRecord> Records { get; set; } = [];
}

public sealed class CustomerRecord
{
    public required string CustomerId { get; set; }
    public required string FullName { get; set; }
    public required string Email { get; set; }
    public DateTime LastUpdatedUtc { get; set; }
}

public sealed class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string ExportRequestTopic { get; set; } = "export-requests";
    public string ExportDataTopic { get; set; } = "export-data";
    public string ExportRequestGroupId { get; set; } = "kafka-data-export-request-group";
    public string ExportDataGroupId { get; set; } = "kafka-data-export-data-group";
}

public sealed class ExportDataRecord
{
    public int Id { get; set; }
    public required string JobId { get; set; }
    public required string CustomerId { get; set; }
    public string Format { get; set; } = "json";
    public string RequestedBy { get; set; } = "anonymous";
    public DateTime GeneratedAtUtc { get; set; }
    public int TotalRows { get; set; }
}
