using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;

namespace CommonModel.Runtime.Drivers.Generic.Adapters;

public abstract class BaseProtocolAdapter : IProtocolAdapter
{
    private bool _isOpen;

    public abstract string SourceType { get; }

    public async Task OpenAsync(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        if (_isOpen) return;
        await OpenCoreAsync(descriptor, ct);
        _isOpen = true;
    }

    public async Task CloseAsync(CancellationToken ct)
    {
        if (!_isOpen) return;
        await CloseCoreAsync(ct);
        _isOpen = false;
    }

    protected abstract Task OpenCoreAsync(ConnectorDescriptor descriptor, CancellationToken ct);
    protected abstract Task CloseCoreAsync(CancellationToken ct);

    public abstract IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(
        ConnectorDescriptor descriptor, CancellationToken ct);

    public abstract IReadOnlyList<string> Validate(ConnectorDescriptor descriptor);

    public virtual ValueTask DisposeAsync()
    {
        _isOpen = false;
        return ValueTask.CompletedTask;
    }
}
