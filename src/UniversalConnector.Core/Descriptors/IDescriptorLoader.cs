namespace UniversalConnector.Core.Descriptors;

public sealed record DescriptorLoadResult
{
    public bool Success { get; init; }
    public ConnectorDescriptor? Descriptor { get; init; }
    public string FilePath { get; init; } = "";
    public string? Error { get; init; }
    public IReadOnlyList<string> Warnings { get; init; } = Array.Empty<string>();

    public static DescriptorLoadResult Ok(ConnectorDescriptor descriptor, string filePath, IReadOnlyList<string>? warnings = null) =>
        new() { Success = true, Descriptor = descriptor, FilePath = filePath, Warnings = warnings ?? Array.Empty<string>() };

    public static DescriptorLoadResult Fail(string filePath, string error) =>
        new() { Success = false, FilePath = filePath, Error = error };
}

public sealed class DescriptorValidationResult
{
    public bool IsValid { get; init; }
    public IReadOnlyList<string> Errors { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> Warnings { get; init; } = Array.Empty<string>();

    public static DescriptorValidationResult Valid(IReadOnlyList<string>? warnings = null) =>
        new() { IsValid = true, Warnings = warnings ?? Array.Empty<string>() };

    public static DescriptorValidationResult Invalid(IReadOnlyList<string> errors, IReadOnlyList<string>? warnings = null) =>
        new() { IsValid = false, Errors = errors, Warnings = warnings ?? Array.Empty<string>() };
}

public interface IDescriptorLoader
{
    Task<IReadOnlyList<DescriptorLoadResult>> LoadFromDirectoryAsync(string directoryPath, CancellationToken ct);
    Task<DescriptorLoadResult> LoadFromFileAsync(string filePath, CancellationToken ct);
    DescriptorLoadResult LoadFromString(string content, string format = "yaml", string filePath = "");
    DescriptorValidationResult Validate(ConnectorDescriptor descriptor);
}
