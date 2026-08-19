using KafkaDataExport.Worker.Models;

namespace KafkaDataExport.Worker.Services;

public sealed class ExportService
{
    private readonly CustomerRepository _customerRepository;

    public ExportService(CustomerRepository customerRepository)
    {
        _customerRepository = customerRepository;
    }

    public async Task<ExportDataMessage> GenerateExportDataAsync(
        ExportRequestMessage request,
        CancellationToken cancellationToken)
    {
        var records = await _customerRepository.GetCustomerDataAsync(request.CustomerId, cancellationToken);

        return new ExportDataMessage
        {
            JobId = request.JobId,
            CustomerId = request.CustomerId,
            Format = request.Format,
            RequestedBy = request.RequestedBy,
            GeneratedAtUtc = DateTime.UtcNow,
            TotalRows = records.Count,
            Records = records
        };
    }
}
