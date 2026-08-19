using System.Text.Json;
using Confluent.Kafka;
using KafkaDataExport.Api.Models;
using Microsoft.Extensions.Options;

namespace KafkaDataExport.Api.Services;

public interface IExportJobService
{
    Task<ExportRequestResponse> QueueExportAsync(ExportRequest request, CancellationToken cancellationToken);
}

public sealed class ExportJobService : IExportJobService
{
    private readonly IProducer<Null, string> _producer;
    private readonly KafkaSettings _kafkaSettings;

    public ExportJobService(IProducer<Null, string> producer, IOptions<KafkaSettings> kafkaOptions)
    {
        _producer = producer;
        _kafkaSettings = kafkaOptions.Value;
    }

    public async Task<ExportRequestResponse> QueueExportAsync(ExportRequest request, CancellationToken cancellationToken)
    {
        var message = new ExportRequestMessage
        {
            JobId = Guid.NewGuid().ToString("N"),
            CustomerId = request.CustomerId,
            Format = request.Format,
            RequestedBy = request.RequestedBy,
            RequestedAtUtc = DateTime.UtcNow
        };

        var payload = JsonSerializer.Serialize(message);

        await _producer.ProduceAsync(
            _kafkaSettings.ExportRequestTopic,
            new Message<Null, string> { Value = payload },
            cancellationToken);

        return new ExportRequestResponse
        {
            JobId = message.JobId,
            Status = "Queued"
        };
    }
}
