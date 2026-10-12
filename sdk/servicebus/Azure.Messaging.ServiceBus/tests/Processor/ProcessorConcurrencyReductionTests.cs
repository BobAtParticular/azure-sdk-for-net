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
            public int Running;
            public int MaxRunning;
            public int Hung;
            public long Receives;
            public long Sequence;
            public Func<long, bool> ShouldHang = _ => false;
            public readonly TaskCompletionSource<bool> ReleaseHungHandlers = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public async Task MeasureAsync(TimeSpan duration)
            {
                Volatile.Write(ref MaxRunning, Volatile.Read(ref Running));
                Interlocked.Exchange(ref Receives, 0);
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
            await Task.Delay(500);

            await ReduceThenRestoreAsync(processor);
            await tracker.MeasureAsync(TimeSpan.FromSeconds(2));

            Assert.That(Volatile.Read(ref tracker.MaxRunning), Is.EqualTo(MaxConcurrentCalls));
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
            await WaitForAsync(() => Volatile.Read(ref tracker.Hung) == hungHandlers);

            await ReduceThenRestoreAsync(processor);
            await tracker.MeasureAsync(TimeSpan.FromSeconds(2));

            Assert.That(Interlocked.Read(ref tracker.Receives), Is.GreaterThan(0), "the processor stopped receiving");
            Assert.That(Volatile.Read(ref tracker.MaxRunning), Is.EqualTo(MaxConcurrentCalls - hungHandlers));
        }

        [Test]
        public async Task ReducedConcurrencyIsHonoredWhenCancelledHandlersComplete()
        {
            var tracker = new HandlerTracker { ShouldHang = sequence => sequence <= 2 };
            var processor = CreateProcessor(tracker);
            await using var cleanup = new ProcessorCleanup(processor, tracker);
            await processor.StartProcessingAsync();
            await WaitForAsync(() => Volatile.Read(ref tracker.Hung) == 2);

            processor.UpdateConcurrency(1);
            await Task.Delay(500);
            tracker.ReleaseHungHandlers.TrySetResult(true);
            await Task.Delay(200);
            await tracker.MeasureAsync(TimeSpan.FromSeconds(2));

            Assert.That(Interlocked.Read(ref tracker.Receives), Is.GreaterThan(0), "the processor stopped receiving");
            Assert.That(Volatile.Read(ref tracker.MaxRunning), Is.EqualTo(1));
        }

        private sealed class ProcessorCleanup : IAsyncDisposable
        {
            private readonly ServiceBusProcessor _processor;
            private readonly HandlerTracker _tracker;

            public ProcessorCleanup(ServiceBusProcessor processor, HandlerTracker tracker)
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
            }
        }

        private static async Task ReduceThenRestoreAsync(ServiceBusProcessor processor)
        {
            processor.UpdateConcurrency(1);
            await Task.Delay(200);
            processor.UpdateConcurrency(MaxConcurrentCalls);
            await Task.Delay(500);
        }

        private static async Task WaitForAsync(Func<bool> condition)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while (!condition())
            {
                await Task.Delay(20, timeout.Token);
            }
        }

        private static ServiceBusProcessor CreateProcessor(HandlerTracker tracker)
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

            var connection = ServiceBusTestUtilities.CreateMockConnection();
            connection
                .Setup(c => c.CreateTransportReceiver(
                    It.IsAny<string>(), It.IsAny<ServiceBusRetryPolicy>(), It.IsAny<ServiceBusReceiveMode>(),
                    It.IsAny<uint>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(),
                    It.IsAny<bool>(), It.IsAny<Guid?>(), It.IsAny<CancellationToken>()))
                .Returns(receiver.Object);

            var processor = new ServiceBusProcessor(connection.Object, "queue", false, new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = MaxConcurrentCalls,
                AutoCompleteMessages = false,
                MaxAutoLockRenewalDuration = TimeSpan.Zero,
            });

            processor.ProcessErrorAsync += _ => Task.CompletedTask;
            processor.ProcessMessageAsync += async args =>
            {
                if (tracker.ShouldHang(args.Message.SequenceNumber))
                {
                    Interlocked.Increment(ref tracker.Hung);
                    // Like a handler blocked on I/O without a timeout: doesn't observe args.CancellationToken.
                    await tracker.ReleaseHungHandlers.Task;
                    return;
                }

                var running = Interlocked.Increment(ref tracker.Running);
                int observed;
                while (running > (observed = Volatile.Read(ref tracker.MaxRunning)) &&
                       Interlocked.CompareExchange(ref tracker.MaxRunning, running, observed) != observed)
                {
                }

                try
                {
                    await Task.Delay(50);
                }
                finally
                {
                    Interlocked.Decrement(ref tracker.Running);
                }
            };

            return processor;
        }
    }
}
