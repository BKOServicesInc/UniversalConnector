using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Core.Abstractions;

public interface IEventPipeline
{
    Task ProcessAsync(RawChangeEvent evt, CancellationToken ct = default);
}
