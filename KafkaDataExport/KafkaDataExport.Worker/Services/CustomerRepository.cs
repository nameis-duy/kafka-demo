using KafkaDataExport.Worker.Models;

namespace KafkaDataExport.Worker.Services;

public sealed class CustomerRepository
{
    public Task<List<CustomerRecord>> GetCustomerDataAsync(string customerId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var now = DateTime.UtcNow;
        var rows = new List<CustomerRecord>
        {
            new()
            {
                CustomerId = customerId,
                FullName = "Nguyen Van A",
                Email = "nguyenvana@example.com",
                LastUpdatedUtc = now
            },
            new()
            {
                CustomerId = customerId,
                FullName = "Tran Thi B",
                Email = "tranthib@example.com",
                LastUpdatedUtc = now.AddMinutes(-5)
            }
        };

        return Task.FromResult(rows);
    }
}
