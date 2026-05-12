using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Infrastructure;

public sealed class LifecycleFsm
{
    // (currentState, action) → nextState
    private static readonly IReadOnlyDictionary<(DriverState State, string Action), DriverState> Transitions =
        new Dictionary<(DriverState, string), DriverState>
        {
            [(DriverState.Disconnected, "start")]   = DriverState.Connecting,
            [(DriverState.Failed,       "start")]   = DriverState.Connecting,
            [(DriverState.Failed,       "restart")] = DriverState.Connecting,
            [(DriverState.Connecting,   "stop")]    = DriverState.Disconnected,
            [(DriverState.Connected,    "stop")]    = DriverState.Disconnected,
            [(DriverState.Streaming,    "stop")]    = DriverState.Disconnected,
            [(DriverState.Streaming,    "restart")] = DriverState.Connecting,
            [(DriverState.Reconnecting, "stop")]    = DriverState.Disconnected,
        };

    public bool CanApply(DriverState state, string action) =>
        Transitions.ContainsKey((state, action.ToLowerInvariant()));

    // Returns the expected next state, or null if the transition is invalid.
    public DriverState? TryApply(DriverState state, string action) =>
        Transitions.TryGetValue((state, action.ToLowerInvariant()), out var next) ? next : null;

    public IReadOnlyCollection<string> ValidActionsFor(DriverState state) =>
        Transitions.Keys
            .Where(k => k.State == state)
            .Select(k => k.Action)
            .ToArray();
}
