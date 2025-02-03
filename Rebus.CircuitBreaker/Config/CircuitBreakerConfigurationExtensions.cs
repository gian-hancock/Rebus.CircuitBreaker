﻿using Rebus.Bus;
using Rebus.CircuitBreaker;
using Rebus.Logging;
using Rebus.Retry;
using Rebus.Threading;
using Rebus.Time;
using System;
using System.Collections.Generic;
using System.Linq;
using Rebus.Injection;

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for the Circuit breakers
/// </summary>
public static class CircuitBreakerConfigurationExtensions
{
    /// <summary>
    /// Enabling fluent configuration of circuit breakers
    /// </summary>
    /// <param name="configurer"></param>
    /// <param name="circuitBreakerBuilder"></param>
    public static void EnableCircuitBreaker(this OptionsConfigurer configurer, Action<CircuitBreakerConfigurationBuilder> circuitBreakerBuilder)
    {
        var builder = new CircuitBreakerConfigurationBuilder();
        circuitBreakerBuilder?.Invoke(builder);

        //capture initial worker count for bus starter compatibility
        var initialWorkerCount = 0;
        configurer.Decorate(c =>
        {
            var options = c.Get<Options>();
            initialWorkerCount = options.NumberOfWorkers;
            return options;
        });

        configurer.Register(_ => new CircuitBreakerEvents());

        configurer.Register(context =>
        {
            var loggerFactory = context.Get<IRebusLoggerFactory>();
            var asyncTaskFactory = context.Get<IAsyncTaskFactory>();
            var circuitBreakerEvents = context.Get<CircuitBreakerEvents>();
            var circuitBreakers = builder.Build(context);

            return new MainCircuitBreaker(circuitBreakers, loggerFactory, asyncTaskFactory, new Lazy<IBus>(context.Get<IBus>), circuitBreakerEvents, initialWorkerCount);
        });

        configurer.Decorate<IErrorTracker>(context =>
        {
            var innerErrorTracker = context.Get<IErrorTracker>();
            var circuitBreaker = context.Get<MainCircuitBreaker>();

            return new CircuitBreakerErrorTracker(innerErrorTracker, circuitBreaker);
        });
    }

    /// <summary>
    /// Configuration builder to fluently register circuit breakers
    /// </summary>
    public class CircuitBreakerConfigurationBuilder
    {
        readonly List<Func<IResolutionContext, ICircuitBreaker>> _circuitBreakerFactories = new();

        /// <summary>
        /// Register a circuit breaker based on an <typeparamref name="TException"/>
        /// </summary>
        /// <typeparam name="TException">Exception type to trip the circuit breaker on</typeparam>
        public CircuitBreakerConfigurationBuilder OpenOn<TException>(
            int attempts = CircuitBreakerSettings.DefaultAttempts,
            int trackingPeriodInSeconds = CircuitBreakerSettings.DefaultTrackingPeriodInSeconds,
            int halfOpenPeriodInSeconds = CircuitBreakerSettings.DefaultHalfOpenResetInterval,
            int resetIntervalInSeconds = CircuitBreakerSettings.DefaultCloseResetInterval,
            ResetMode resetMode = ResetMode.WhileAnyState
        )
            where TException : Exception
        {
            return OpenOn<TException>(attempts, trackingPeriodInSeconds, _ => TimeSpan.FromSeconds(halfOpenPeriodInSeconds), resetIntervalInSeconds, resetMode);
        }

        /// <summary>
        /// Register a circuit breaker based on an <typeparamref name="TException"/>
        /// </summary>
        /// <param name="halfOpenPeriodProvider">
        ///     A callback which provides the number of seconds to wait before transitioning to the half open state. The number of times 
        ///     the circuit breaker has entered the HalfOpen state consecutively is passed as a parameter to this callback, starting at 0 
        ///     and being reset once the circuit breaker enters the closed state.
        /// </param>
        /// <typeparam name="TException">Exception type to trip the circuit breaker on</typeparam>
        public CircuitBreakerConfigurationBuilder OpenOn<TException>(
            int attempts,
            int trackingPeriodInSeconds,
            Func<int, TimeSpan> halfOpenPeriodProvider,
            int resetIntervalInSeconds = CircuitBreakerSettings.DefaultCloseResetInterval,
            ResetMode resetMode = ResetMode.WhileAnyState)
            where TException : Exception
        {
            halfOpenPeriodProvider ??= _ => TimeSpan.FromSeconds(CircuitBreakerSettings.DefaultHalfOpenResetInterval);

            var settings = new CircuitBreakerSettings(attempts, trackingPeriodInSeconds, halfOpenPeriodProvider, resetIntervalInSeconds, resetMode);

            _circuitBreakerFactories.Add(context => new ExceptionTypeCircuitBreaker(typeof(TException), settings, context.Get<IRebusTime>()));

            return this;
        }

        internal IList<ICircuitBreaker> Build(IResolutionContext context) => _circuitBreakerFactories.Select(factory => factory(context)).ToList();
    }
}