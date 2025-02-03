using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Transport.InMem;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
// ReSharper disable ArgumentsStyleAnonymousFunction
#pragma warning disable 1998

namespace Rebus.CircuitBreaker.Tests.CircuitBreaker;

[TestFixture]
public class CircuitBreakerTests : FixtureBase
{
    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task OpensCircuitBreakerOnException(bool useBusStarter)
    {
        var bus = ConfigureBus(
            handlers: a => a.Handle<string>(async _ => throw new MyCustomException()),
            options: o => o.EnableCircuitBreaker(c => c.OpenOn<MyCustomException>(attempts: 1, trackingPeriodInSeconds: 10)),
            useBusStarter
            );

        await bus.SendLocal("Uh oh, This is not gonna go well!");

        await Task.Delay(TimeSpan.FromSeconds(5));

        var workerCount = bus.Advanced.Workers.Count;

        Assert.That(workerCount, Is.EqualTo(0), $"Expected worker count to be '0' but was {workerCount}");
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task OpensCircuitBreakerAgainAfterLittleWhile(bool useBusStarter)
    {
        var deliveryCount = 0;

        var bus = ConfigureBus(
            handlers: a => a.Handle<string>(async _ =>
            {
                deliveryCount++;

                if (deliveryCount > 1)
                {
                    Console.WriteLine($"Handling message properly this time");
                    return;
                }

                throw new MyCustomException();
            }),
            options: o =>
            {
                o.EnableCircuitBreaker(c => c.OpenOn<MyCustomException>(attempts: 1, trackingPeriodInSeconds: 10, halfOpenPeriodInSeconds: 20, resetIntervalInSeconds: 30));
            },
            useBusStarter
        );

        await bus.SendLocal("Uh oh, This is not gonna go well!");

        await Task.Delay(TimeSpan.FromSeconds(35));

        var workerCount = bus.Advanced.Workers.Count;

        Assert.That(workerCount, Is.EqualTo(1), $"Expected worker count to be '1' after waiting the entire reset interval plus some more, but was {workerCount}");
    }
    
    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task WaitHalfOpenPeriodBeforeHalfOpening(bool useBusStarter)
    {
        var stateChanges = new List<CircuitBreakerState>();
        var events = new CircuitBreakerEvents();
        events.CircuitBreakerChanged += (CircuitBreakerState newState) =>
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Circuit breaker state changed {stateChanges.LastOrDefault()} -> {newState}");
            stateChanges.Add(newState);
        };

        var deliveryCount = 0;

        var bus = ConfigureBus(
            handlers: a => a.Handle<string>(async _ =>
            {
                deliveryCount++;
                await Task.Delay(500); // Make handler take reasonable amount of time.
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] DeliveryCount: {deliveryCount}");
                throw new MyCustomException();
            }),
            options: o =>
            {
                o.EnableCircuitBreaker(c => c.OpenOn<MyCustomException>(attempts: 1, trackingPeriodInSeconds: 10, halfOpenPeriodInSeconds: 20, resetIntervalInSeconds: 30));
                o.Decorate(c => events);
            },
            useBusStarter
        );

        await bus.SendLocal("Uh oh, This is not gonna go well!");

        await Task.Delay(TimeSpan.FromSeconds(10));
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Assert state changes -> open");
        Assert.That(stateChanges, Is.EquivalentTo(new CircuitBreakerState[] { CircuitBreakerState.Open }), $"[{DateTime.Now:HH:mm:ss.fff}] Expect state transition -> open after first error but before halfOpenPeriod");

        await Task.Delay(TimeSpan.FromSeconds(20));
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Assert state changes -> open -> half open -> open");
        Assert.That(stateChanges, Is.EquivalentTo(new CircuitBreakerState[] { CircuitBreakerState.Open, CircuitBreakerState.HalfOpen, CircuitBreakerState.Open }), $"[{DateTime.Now:HH:mm:ss.fff}] Expect state transitions -> open -> half open -> open after halfOpenPeriod");
    }

    [Test]
    public async Task WaitDynamicHalfOpenPeriodBeforeHalfOpening()
    {
        const int TimingMargin = 2;

        var stateChanges = new List<CircuitBreakerState>();
        var events = new CircuitBreakerEvents();
        events.CircuitBreakerChanged += stateChanges.Add;

        var deliveryCount = 0;
        bool handlerWillThrow = true;
      
        var bus = ConfigureBus(
            handlers: a => a.Handle<string>(async _ =>
            {
                deliveryCount++;
                await Task.Delay(100); // Make handler take reasonable amount of time.
                if (handlerWillThrow) throw new MyCustomException();
            }),
            options: o =>
            {
                o.EnableCircuitBreaker(
                    c => c.OpenOn<MyCustomException>(
                        attempts: 1, 
                        trackingPeriodInSeconds: 5,
                        halfOpenPeriodProvider: attemptCount => TimeSpan.FromSeconds(4 + (attemptCount % 2) * 4), // 5, 10, 5, 10, etc...
                        resetIntervalInSeconds: 2,
                        resetMode: ResetMode.WhileHalfOpen));
                o.Decorate(c => events);
            },
            false
        );

        await bus.SendLocal("Uh oh, This is not gonna go well!");

        // Expect state changes according to sequence of halfOpen periods: 5s, 10s, 5s, 10s, etc...

        Assert.That(
            () => stateChanges, 
            Is.EquivalentTo(new CircuitBreakerState[] { CircuitBreakerState.Open }).After(2500).PollEvery(100),
            "Expect circuit opens after failure");
        stateChanges.Clear();

        Assert.That(
            () => stateChanges,
            Is.EquivalentTo(new CircuitBreakerState[] { CircuitBreakerState.HalfOpen, CircuitBreakerState.Open }).After(4 + TimingMargin).Seconds.PollEvery(100),
            "Expect circuit half opens then opens again after half open period of 4s");
        stateChanges.Clear();
        Stopwatch stopwatch = Stopwatch.StartNew();

        Assert.That(
            () => stateChanges,
            Is.EquivalentTo(new CircuitBreakerState[] { CircuitBreakerState.HalfOpen, CircuitBreakerState.Open }).After(8 + TimingMargin).Seconds.PollEvery(100),
            "Expect circuit half opens then opens again after half open period of 8s");
        Assert.That(
            stopwatch.ElapsedMilliseconds,
            Is.GreaterThan(8000 - TimingMargin * 1000),
            "Expect half open period to take close to 8 seconds");
        stateChanges.Clear();

        Assert.That(
            () => stateChanges,
            Is.EquivalentTo(new CircuitBreakerState[] { CircuitBreakerState.HalfOpen, CircuitBreakerState.Open }).After(4 + TimingMargin).Seconds.PollEvery(100),
            "Expect circuit half opens then opens again after half open period of 4s");
        stateChanges.Clear();

        handlerWillThrow = false;
        Assert.That(
            () => stateChanges,
            Is.EquivalentTo(new CircuitBreakerState[] { CircuitBreakerState.HalfOpen, CircuitBreakerState.Closed }).After(8 + 2 + TimingMargin).Seconds.PollEvery(100),
            "Expect circuit half opens, message succeeds, and closes again after half open period of 8s + reset interval of 2s");
        stateChanges.Clear();
    }

    [Test]
    public async Task DynamicHalfOpenPeriodResets()
    {
        int lastAttemptCount = 0;
        int deliveryCount = 0;
        var handlerWillThrow = true;
        var stateChanges = new List<CircuitBreakerState>();

        var events = new CircuitBreakerEvents();
        events.CircuitBreakerChanged += (CircuitBreakerState newState) =>
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Circuit breaker state changed {stateChanges.LastOrDefault()} -> {newState}");
            stateChanges.Add(newState);
        };

        var bus = ConfigureBus(
            handlers: a => a.Handle<string>(async _ =>
            {
                deliveryCount++;
                await Task.Delay(100); // Make handler take reasonable amount of time.
                if (handlerWillThrow) throw new MyCustomException();
            }),
            options: o =>
            {
                o.EnableCircuitBreaker(
                    c => c.OpenOn<MyCustomException>(
                        attempts: 1,
                        trackingPeriodInSeconds: 10,
                        halfOpenPeriodProvider: attemptCount => {
                            lastAttemptCount = attemptCount;
                            return TimeSpan.FromSeconds(1);
                        },
                        resetIntervalInSeconds: 1,
                        resetMode: ResetMode.WhileHalfOpen));
                o.Decorate(c => events);
            },
            false
        );

        handlerWillThrow = true;
        await bus.SendLocal("Uh oh, This is not gonna go well!");
        Assert.That(() => lastAttemptCount, Is.EqualTo(2).After(10).Seconds.PollEvery(100));
        
        handlerWillThrow = false;

        // Wait for circuit breaker to close
        stateChanges.Clear();
        Assert.That(() => stateChanges, Does.Contain(CircuitBreakerState.Closed).After(10).Seconds.PollEvery(100));

        handlerWillThrow = true;
        await bus.SendLocal("Uh oh, This is not gonna go well!");

        // Expect attempt count to be reset
        Assert.That(() => lastAttemptCount, Is.EqualTo(1).After(10).Seconds.PollEvery(100));
    }

    [Test]
    public async Task ResetModeWhileHalfOpenPreventsClosingWhileOpen()
    {
        const int HalfOpenPeriod = 8;
        const int ResetInterval = 8;
        const int TimingMargin = 2;

        var stateChanges = new List<CircuitBreakerState>();
        var events = new CircuitBreakerEvents();
        events.CircuitBreakerChanged += (CircuitBreakerState newState) =>
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Circuit breaker state changed {stateChanges.LastOrDefault()} -> {newState}");
            stateChanges.Add(newState);
        };

        bool handlerWillThrow = true;
      
        var bus = ConfigureBus(
            handlers: a => a.Handle<string>(async _ =>
            {
                await Task.Delay(100); // Make handler take reasonable amount of time.
                if (handlerWillThrow) throw new MyCustomException();
            }),
            options: o =>
            {
                o.EnableCircuitBreaker(
                    c => c.OpenOn<MyCustomException>(
                        attempts: 1, 
                        trackingPeriodInSeconds: 10,
                        halfOpenPeriodInSeconds: HalfOpenPeriod,
                        resetIntervalInSeconds: ResetInterval,
                        resetMode: ResetMode.WhileHalfOpen));
                o.Decorate(c => events);
            },
            false
        );

        await bus.SendLocal("Uh oh, This is not gonna go well!");

        Assert.That(
            () => stateChanges, 
            Is.EquivalentTo(new CircuitBreakerState[] { CircuitBreakerState.Open }).After(TimingMargin).Seconds.PollEvery(100),
            "Expect circuit opens after failure");
        stateChanges.Clear();

        handlerWillThrow = false;

        Assert.That(
            () => stateChanges,
            Is.EquivalentTo(new CircuitBreakerState[] { }).After(HalfOpenPeriod - TimingMargin).Seconds,
            "Expect circuit remains open for halfOpenPeriod");
        stateChanges.Clear();

        Assert.That(
            () => stateChanges,
            Is.EquivalentTo(new CircuitBreakerState[] { CircuitBreakerState.HalfOpen }).After(TimingMargin*2).Seconds.PollEvery(100),
            "Expect circuit to transition to halfOpen after halfOpenPeriod");
        stateChanges.Clear();

        Assert.That(
            () => stateChanges,
            Is.EquivalentTo(new CircuitBreakerState[] { }).After(ResetInterval - TimingMargin).Seconds,
            "Expect circuit remains halfOpen within resetPeriod");
        stateChanges.Clear();

        Assert.That(
            () => stateChanges,
            Is.EquivalentTo(new CircuitBreakerState[] { CircuitBreakerState.Closed }).After(TimingMargin * 2).Seconds.PollEvery(100),
            "Expect circuit to transition to closed after resetPeriod");
        stateChanges.Clear();
    }


    IBus ConfigureBus(Action<BuiltinHandlerActivator> handlers, Action<OptionsConfigurer> options, bool useBusStarter)
    {
        var network = new InMemNetwork();
        var activator = Using(new BuiltinHandlerActivator());

        handlers(activator);

        var configurer = Configure.With(activator)
            .Logging(l => l.Console(minLevel: LogLevel.Debug))
            .Transport(t => t.UseInMemoryTransport(network, "queue-a"))
            .Options(options);

        if (useBusStarter)
        {
            var starter = configurer.Create();
            return starter.Start();
        }

        return configurer.Start();
    }

    class MyCustomException : Exception { }
}