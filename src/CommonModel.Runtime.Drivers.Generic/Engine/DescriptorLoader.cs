using System.Text.Json;
using System.Text.RegularExpressions;
using CommonModel.Runtime.Core.Descriptors;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace CommonModel.Runtime.Drivers.Generic.Engine;

public sealed class DescriptorLoader : IDescriptorLoader
{
    private readonly DescriptorValidator _validator;
    private static readonly Regex EnvVarPattern = new(@"\$\{([^}]+)\}", RegexOptions.Compiled);

    private static readonly IDeserializer YamlDeserializer = new DeserializerBuilder()
        .WithNamingConvention(CamelCaseNamingConvention.Instance)
        .IgnoreUnmatchedProperties()
        .Build();

    public DescriptorLoader(DescriptorValidator validator) => _validator = validator;

    public async Task<IReadOnlyList<DescriptorLoadResult>> LoadFromDirectoryAsync(
        string directoryPath, CancellationToken ct)
    {
        if (!Directory.Exists(directoryPath))
            return Array.Empty<DescriptorLoadResult>();

        var files = Directory.GetFiles(directoryPath, "*.yaml", SearchOption.TopDirectoryOnly)
            .Concat(Directory.GetFiles(directoryPath, "*.yml", SearchOption.TopDirectoryOnly))
            .Concat(Directory.GetFiles(directoryPath, "*.json", SearchOption.TopDirectoryOnly))
            .ToArray();

        var results = new List<DescriptorLoadResult>(files.Length);
        foreach (var file in files)
        {
            ct.ThrowIfCancellationRequested();
            results.Add(await LoadFromFileAsync(file, ct));
        }
        return results;
    }

    public async Task<DescriptorLoadResult> LoadFromFileAsync(string filePath, CancellationToken ct)
    {
        try
        {
            var content = await File.ReadAllTextAsync(filePath, ct);
            var format = Path.GetExtension(filePath).TrimStart('.').ToLowerInvariant() == "json" ? "json" : "yaml";
            return LoadFromString(content, format, filePath);
        }
        catch (Exception ex)
        {
            return DescriptorLoadResult.Fail(filePath, ex.Message);
        }
    }

    public DescriptorLoadResult LoadFromString(string content, string format = "yaml", string filePath = "")
    {
        try
        {
            var interpolated = InterpolateEnvVars(content);
            ConnectorDescriptor descriptor;

            if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
            {
                descriptor = JsonSerializer.Deserialize<ConnectorDescriptor>(interpolated,
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                    ?? throw new InvalidOperationException("Descriptor deserialized to null");
            }
            else
            {
                descriptor = YamlDeserializer.Deserialize<ConnectorDescriptor>(interpolated)
                    ?? throw new InvalidOperationException("Descriptor deserialized to null");
            }

            var validation = _validator.Validate(descriptor);
            if (!validation.IsValid)
                return DescriptorLoadResult.Fail(filePath, string.Join("; ", validation.Errors));

            return DescriptorLoadResult.Ok(descriptor, filePath, validation.Warnings);
        }
        catch (Exception ex)
        {
            return DescriptorLoadResult.Fail(filePath, ex.Message);
        }
    }

    public DescriptorValidationResult Validate(ConnectorDescriptor descriptor) =>
        _validator.Validate(descriptor);

    private static string InterpolateEnvVars(string content)
    {
        return EnvVarPattern.Replace(content, match =>
        {
            var varName = match.Groups[1].Value;
            var value = Environment.GetEnvironmentVariable(varName);
            if (value is null)
                throw new InvalidOperationException($"Environment variable '{varName}' is not set");
            return value;
        });
    }
}
