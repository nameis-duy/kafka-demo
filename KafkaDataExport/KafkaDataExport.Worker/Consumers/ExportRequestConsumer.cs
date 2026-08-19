using System.Text.Json;
using Confluent.Kafka;
using KafkaDataExport.Worker.Models;
using KafkaDataExport.Worker.Services;
using Microsoft.Extensions.Options;

namespace KafkaDataExport.Worker.Consumers;

public sealed class ExportRequestConsumer : BackgroundService
{
    private readonly ILogger<ExportRequestConsumer> _logger;
    private readonly KafkaSettings _kafkaSettings;
    private readonly ExportService _exportService;
    private readonly IProducer<Null, string> _producer;

    public ExportRequestConsumer(
        ILogger<ExportRequestConsumer> logger,
        IOptions<KafkaSettings> kafkaOptions,
        ExportService exportService,
        IProducer<Null, string> producer)
    {
        _logger = logger;
        _kafkaSettings = kafkaOptions.Value;
        _exportService = exportService;
        _producer = producer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            GroupId = _kafkaSettings.ExportRequestGroupId + "-duy-local",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(_kafkaSettings.ExportRequestTopic);

        _logger.LogInformation("ExportRequestConsumer subscribed to topic {Topic}", _kafkaSettings.ExportRequestTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(stoppingToken);
                var request = JsonSerializer.Deserialize<ExportRequestMessage>(consumeResult.Message.Value);

                if (request is null)
                {
                    _logger.LogWarning("Received invalid export request message.");
                    consumer.Commit(consumeResult);
                    continue;
                }

                var exportData = await _exportService.GenerateExportDataAsync(request, stoppingToken);
                var payload = JsonSerializer.Serialize(exportData);

                await _producer.ProduceAsync(
                    _kafkaSettings.ExportDataTopic,
                    new Message<Null, string> { Value = payload },
                    stoppingToken);

                consumer.Commit(consumeResult);

                _logger.LogInformation(
                    "Processed export request for job {JobId} and published to {Topic}",
                    request.JobId,
                    _kafkaSettings.ExportDataTopic);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("ExportRequestConsumer is stopping.");
        }
        finally
        {
            consumer.Close();
        }
    }
}
