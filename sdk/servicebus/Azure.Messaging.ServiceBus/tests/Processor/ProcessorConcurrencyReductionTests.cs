// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus.Core;
using Moq;
using NUnit.Framework;

namespace Azure.Messaging.ServiceBus.Tests.Processor
{
    /// <summary>
    /// Covers <see cref="ServiceBusProcessor.UpdateConcurrency(int)"/> when lowering concurrency while handlers that
    /// don't observe <see cref="ProcessMessageEventArgs.CancellationToken"/> are still running.
    /// </summary>
    [NonParallelizable]
    public class ProcessorConcurrencyReductionTests
    {
        private const int MaxConcurrentCalls = 4;

        private sealed class HandlerTracker
        {
            private readonly object _handlerLock = new();
            private readonly List<CancellationToken> _runningHandlerTokens = new();

            public int Running;
            public int MaxRunning;
            public int Hung;
            public long Receives;
            public long Sequence;
            public int ObservedPrefetchCount;
            public int NextPrefetchCount;
            public Func<long, bool> ShouldHang = _ => false;
            public readonly TaskCompletionSource<bool> ReleaseHungHandlers = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public bool HasCancelledHandlers
            {
                get
                {
                    lock (_handlerLock)
                    {
                        return _runningHandlerTokens.Exists(token => token.IsCancellationRequested);
                    }
                }
            }

            public bool TryStartHandler(CancellationToken cancellationToken)
            {
                lock (_handlerLock)
                {
                    // A receive may deliver a message just as its task is cancelled. Do not start another ordinary handler.
                    if (cancellationToken.IsCancellationRequested)
                    {
                        return false;
                    }
                    _runningHandlerTokens.Add(cancellationToken);
                    int running = Interlocked.Increment(ref Running);
                    MaxRunning = Math.Max(MaxRunning, running);
                    return true;
                }
            }

            public void CompleteHandler(CancellationToken cancellationToken)
            {
                lock (_handlerLock)
                {
                    _runningHandlerTokens.Remove(cancellationToken);
                    Interlocked.Decrement(ref Running);
                }
            }

            public async Task MeasureAsync(TimeSpan duration)
            {
                // Keep the peak reset atomic with handler entry/exit so a stale peak cannot cross measurement windows.
                lock (_handlerLock)
                {
                    Interlocked.Exchange(ref MaxRunning, Volatile.Read(ref Running));
                    Interlocked.Exchange(ref Receives, 0);
                }
                await Task.Delay(duration);
            }
        }

        [Test]
        public async Task RecoversFullConcurrencyAfterReductionWithoutHungHandlers()
        {
            var tracker = new HandlerTracker();
            var processor = CreateProcessor(tracker);
            await using var cleanup = new ProcessorCleanup(processor, tracker);
            await processor.StartProcessingAsync();
            await AssertConcurrencyAsync(tracker, MaxConcurrentCalls);

            await ReduceThenRestoreAsync(processor, tracker);
            await AssertConcurrencyAsync(tracker, MaxConcurrentCalls);
        }

        [Test]
        public async Task HungHandlersCostOnlyTheirOwnSlotsAfterReductionAndRestore(
            [Values(1, 2)] int hungHandlers,
            [Values(0, 1)] int firstHungSequenceOffset)
        {
            // Which receive tasks a reduction cancels depends on start order, so vary which messages hang to place
            // hung handlers both in cancelled and in surviving receive tasks.
            var tracker = new HandlerTracker
            {
                ShouldHang = sequence => sequence > firstHungSequenceOffset && sequence <= firstHungSequenceOffset + hungHandlers
            };
            var processor = CreateProcessor(tracker);
            await using var cleanup = new ProcessorCleanup(processor, tracker);
            await processor.StartProcessingAsync();
            await WaitForAsync(() => Volatile.Read(ref tracker.Hung) == hungHandlers, "The expected handlers did not enter the hung state.");

            await ReduceThenRestoreAsync(processor, tracker);
            await AssertConcurrencyAsync(tracker, MaxConcurrentCalls - hungHandlers);
        }

        [Test]
        public async Task ReducedConcurrencyIsHonoredWhenCancelledHandlersComplete()
        {
            var tracker = new HandlerTracker { ShouldHang = sequence => sequence <= 2 };
            var processor = CreateProcessor(tracker);
            await using var cleanup = new ProcessorCleanup(processor, tracker);
            await processor.StartProcessingAsync();
            await WaitForAsync(() => Volatile.Read(ref tracker.Hung) == 2, "The expected handlers did not enter the hung state.");

            await UpdateConcurrencyAndWaitAsync(processor, tracker, 1);
            tracker.ReleaseHungHandlers.TrySetResult(true);
            await AssertConcurrencyAsync(tracker, 1);
        }

        [Test]
        public async Task RepeatedReductionsHonorLimitAndRecover(
            [Values(1, 2, 3)] int hungHandlers,
            [Values(0, 1, 2)] int firstHungSequenceOffset)
        {
            var tracker = new HandlerTracker
            {
                ShouldHang = sequence => sequence > firstHungSequenceOffset && sequence <= firstHungSequenceOffset + hungHandlers
            };
            var processor = CreateProcessor(tracker);
            await using var cleanup = new ProcessorCleanup(processor, tracker);
            await processor.StartProcessingAsync();
            await WaitForAsync(() => Volatile.Read(ref tracker.Hung) == hungHandlers, "The expected handlers did not enter the hung state.");

            foreach (int limit in new[] { 3, 2, 4, 2, 4 })
            {
                await UpdateConcurrencyAndWaitAsync(processor, tracker, limit);
                if (limit == MaxConcurrentCalls)
                {
                    await AssertConcurrencyAsync(tracker, MaxConcurrentCalls - hungHandlers);
                }
                else
                {
                    // Already-running hung handlers cannot be forcibly stopped. Check the remaining handlers against
                    // the new limit, including when hung handlers alone outnumber that limit.
                    await AssertConcurrencyDoesNotExceedAsync(tracker, limit);
                }
            }
        }

        [Test]
        public async Task StopAndRestartPreservesOwedPermits()
        {
            var tracker = new HandlerTracker { ShouldHang = sequence => sequence <= 2 };
            var processor = CreateProcessor(tracker);
            await using var cleanup = new ProcessorCleanup(processor, tracker);
            await processor.StartProcessingAsync();
            await WaitForAsync(() => Volatile.Read(ref tracker.Hung) == 2, "The expected handlers did not enter the hung state.");

            await UpdateConcurrencyAndWaitAsync(processor, tracker, 1);
            var stop = processor.StopProcessingAsync();
            await WaitForAsync(() => Volatile.Read(ref tracker.Running) == 0, "Non-hung handlers did not stop.");
            Assert.That(stop.IsCompleted, Is.False, "Stopping must wait for the hung handlers to return.");
            tracker.ReleaseHungHandlers.TrySetResult(true);
            await WaitForAsync(() => stop.IsCompleted, "The processor did not stop after the hung handlers returned.");
            await stop;

            await processor.StartProcessingAsync();
            await AssertConcurrencyAsync(tracker, 1);
            await UpdateConcurrencyAndWaitAsync(processor, tracker, 3);
            await AssertConcurrencyAsync(tracker, 3);
        }

        [Test]
        public async Task SessionProcessorRecoversWithHungHandlers()
        {
            var tracker = new HandlerTracker { ShouldHang = sequence => sequence <= 2 };
            var processor = new ServiceBusSessionProcessor(
                CreateConnection(tracker), "queue", new ServiceBusSessionProcessorOptions
                {
                    MaxConcurrentSessions = 1,
                    MaxConcurrentCallsPerSession = MaxConcurrentCalls,
                    SessionIds = { "session" },
                    AutoCompleteMessages = false,
                    MaxAutoLockRenewalDuration = TimeSpan.Zero,
                });
            processor.ProcessErrorAsync += _ => Task.CompletedTask;
            processor.ProcessMessageAsync += args => ProcessMessageAsync(tracker, args.Message, args.CancellationToken);
            await using var cleanup = new ProcessorCleanup(processor, tracker);
            await processor.StartProcessingAsync();
            await WaitForAsync(() => Volatile.Read(ref tracker.Hung) == 2, "The session handlers did not enter the hung state.");

            processor.UpdateConcurrency(1, 1);
            await WaitForReconciliationAsync(processor.InnerProcessor, tracker);
            await AssertConcurrencyDoesNotExceedAsync(tracker, 1);
            processor.UpdateConcurrency(1, MaxConcurrentCalls);
            await WaitForReconciliationAsync(processor.InnerProcessor, tracker);
            await AssertConcurrencyAsync(tracker, MaxConcurrentCalls - 2);
        }

        private sealed class ProcessorCleanup : IAsyncDisposable
        {
            private readonly IAsyncDisposable _processor;
            private readonly HandlerTracker _tracker;

            public ProcessorCleanup(IAsyncDisposable processor, HandlerTracker tracker)
            {
                _processor = processor;
                _tracker = tracker;
            }

            public async ValueTask DisposeAsync()
            {
                _tracker.ReleaseHungHandlers.TrySetResult(true);

                // A wedged scheduler can keep the processor from stopping, so bound the cleanup to let the test report its failure.
                var dispose = _processor.DisposeAsync().AsTask();
                if (await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(10))) != dispose)
                {
                    Assert.Fail("The processor did not stop within 10 seconds.");
                }

                await dispose;
            }
        }

        private static async Task ReduceThenRestoreAsync(ServiceBusProcessor processor, HandlerTracker tracker)
        {
            await UpdateConcurrencyAndWaitAsync(processor, tracker, 1);
            await AssertConcurrencyDoesNotExceedAsync(tracker, 1);
            await UpdateConcurrencyAndWaitAsync(processor, tracker, MaxConcurrentCalls);
        }

        private static async Task UpdateConcurrencyAndWaitAsync(ServiceBusProcessor processor, HandlerTracker tracker, int limit)
        {
            processor.UpdateConcurrency(limit);
            await WaitForReconciliationAsync(processor, tracker);
        }

        private static async Task WaitForReconciliationAsync(ServiceBusProcessor processor, HandlerTracker tracker)
        {
            // The transport prefetch setter runs after concurrency reconciliation. Use a distinct value as an observable
            // acknowledgement instead of sleeping or reading the scheduler's private state. The mock does not prefetch.
            int prefetchCount = ++tracker.NextPrefetchCount;
            processor.UpdatePrefetchCount(prefetchCount);
            await WaitForAsync(() => Volatile.Read(ref tracker.ObservedPrefetchCount) == prefetchCount,
                "The processor did not reconcile the concurrency update.");
        }

        private static async Task AssertConcurrencyDoesNotExceedAsync(HandlerTracker tracker, int limit)
        {
            await WaitForAsync(() => !tracker.HasCancelledHandlers && Volatile.Read(ref tracker.Running) <= limit,
                "Cancelled ordinary handlers did not return and settle below the reduced limit.");
            await tracker.MeasureAsync(TimeSpan.FromMilliseconds(500));
            Assert.That(Volatile.Read(ref tracker.MaxRunning), Is.LessThanOrEqualTo(limit), "Handlers exceeded the concurrency limit.");
        }

        private static async Task AssertConcurrencyAsync(HandlerTracker tracker, int expected)
        {
            await WaitForAsync(() => !tracker.HasCancelledHandlers && Volatile.Read(ref tracker.Running) == expected,
                $"The processor did not reach {expected} concurrent non-hung handlers.");
            await tracker.MeasureAsync(TimeSpan.FromMilliseconds(500));
            Assert.That(Volatile.Read(ref tracker.MaxRunning), Is.LessThanOrEqualTo(expected), "Handlers exceeded the concurrency limit.");
            Assert.That(Interlocked.Read(ref tracker.Receives), Is.GreaterThan(0), "The processor stopped receiving.");
        }

        private static async Task WaitForAsync(Func<bool> condition, string failureMessage)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            while (!condition())
            {
                if (timeout.IsCancellationRequested)
                {
                    Assert.Fail(failureMessage);
                }
                await Task.Delay(10);
            }
        }

        private static ServiceBusProcessor CreateProcessor(HandlerTracker tracker)
        {
            var processor = new ServiceBusProcessor(CreateConnection(tracker), "queue", false, new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = MaxConcurrentCalls,
                AutoCompleteMessages = false,
                MaxAutoLockRenewalDuration = TimeSpan.Zero,
            });

            processor.ProcessErrorAsync += _ => Task.CompletedTask;
            processor.ProcessMessageAsync += args => ProcessMessageAsync(tracker, args.Message, args.CancellationToken);
            return processor;
        }

        private static ServiceBusConnection CreateConnection(HandlerTracker tracker)
        {
            var receiver = new Mock<TransportReceiver>();
            receiver
                .Setup(r => r.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
                .Returns(async (int _, TimeSpan? __, CancellationToken cancellationToken) =>
                {
                    Interlocked.Increment(ref tracker.Receives);
                    await Task.Delay(5, cancellationToken);
                    var sequence = Interlocked.Increment(ref tracker.Sequence);
                    return (IReadOnlyList<ServiceBusReceivedMessage>)new List<ServiceBusReceivedMessage>
                    {
                        ServiceBusModelFactory.ServiceBusReceivedMessage(
                            messageId: sequence.ToString(),
                            sequenceNumber: sequence,
                            lockTokenGuid: Guid.NewGuid(),
                            lockedUntil: DateTimeOffset.UtcNow.AddMinutes(5))
                    };
                });
            receiver.SetupGet(r => r.SessionId).Returns("session");
            receiver.SetupGet(r => r.SessionLockedUntil).Returns(() => DateTimeOffset.UtcNow.AddMinutes(5));
            receiver.SetupSet(r => r.PrefetchCount = It.IsAny<int>())
                .Callback<int>(count => Volatile.Write(ref tracker.ObservedPrefetchCount, count));
            return ServiceBusTestUtilities.GetMockedReceiverConnection(receiver);
        }

        private static async Task ProcessMessageAsync(HandlerTracker tracker, ServiceBusReceivedMessage message, CancellationToken cancellationToken)
        {
            if (tracker.ShouldHang(message.SequenceNumber))
            {
                Interlocked.Increment(ref tracker.Hung);
                try
                {
                    // Like a handler blocked on I/O without a timeout: does not observe the cancellation token.
                    await tracker.ReleaseHungHandlers.Task;
                }
                finally
                {
                    Interlocked.Decrement(ref tracker.Hung);
                }
                return;
            }

            if (!tracker.TryStartHandler(cancellationToken))
            {
                return;
            }

            try
            {
                await Task.Delay(50);
            }
            finally
            {
                tracker.CompleteHandler(cancellationToken);
            }
        }
    }
}
