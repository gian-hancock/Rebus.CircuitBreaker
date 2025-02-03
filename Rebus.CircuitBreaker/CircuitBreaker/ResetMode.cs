namespace Rebus.CircuitBreaker;

/// <summary>
/// Describes how the circuit breaker resets to a closed state
/// </summary>
public enum ResetMode
{
    /// <summary>
    /// The circuit can reset (close) from any state. 
    /// The reset interval is measured from the most recent error
    /// </summary>
    WhileAnyState,

    /// <summary>
    /// The circuit can reset (close) only when in the HalfOpen state. 
    /// The reset interval is measured from the later of:
    /// - The most recent error.
    /// - The moment the circuit entered the HalfOpen state.
    /// </summary>
    WhileHalfOpen,
}