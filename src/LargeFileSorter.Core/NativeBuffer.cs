using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LargeFileSorter.Core;

/// <summary>
/// Growable array backed by <see cref="NativeMemory.AlignedAlloc"/> —
/// completely invisible to the garbage collector.
///
/// Motivation: a managed <c>EntryIndex[]</c> with 200 M entries (~5.6 GB) creates
/// a single large object root that the GC must scan during every collection.
/// Native memory eliminates that overhead entirely — the GC has zero knowledge
/// of this buffer, producing zero pauses regardless of buffer size.
///
/// Alignment: 64 bytes matches the typical cache line size, ensuring that
/// SIMD loads and sequential iteration don't straddle cache line boundaries.
/// </summary>
internal sealed unsafe class NativeBuffer<T> : IDisposable where T : unmanaged
{
    private T* _ptr;
    private int _count;
    private int _capacity;
    private bool _disposed;

    public NativeBuffer(int initialCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(initialCapacity, 1);
        _capacity = initialCapacity;
        _ptr = (T*)NativeMemory.AlignedAlloc(
            (nuint)((long)_capacity * sizeof(T)), 64);
    }

    public int Count => _count;
    public int Capacity => _capacity;

    public ref T this[int index]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            if ((uint)index >= (uint)_count)
                ThrowOutOfRange(index);
            return ref _ptr[index];
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(in T item)
    {
        if (_count >= _capacity)
            Grow();
        _ptr[_count++] = item;
    }

    /// <summary>Returns a span over the populated portion of the buffer.</summary>
    public Span<T> AsSpan() => new(_ptr, _count);

    /// <summary>Returns a span over a specific range within the buffer.</summary>
    public Span<T> AsSpan(int start, int length) => new(_ptr + start, length);

    /// <summary>Resets the count without freeing or zeroing memory.</summary>
    public void Clear() => _count = 0;

    private void Grow()
    {
        var newCapacity = checked(_capacity * 2);
        var newPtr = (T*)NativeMemory.AlignedAlloc(
            (nuint)((long)newCapacity * sizeof(T)), 64);

        new Span<T>(_ptr, _count).CopyTo(new Span<T>(newPtr, newCapacity));

        NativeMemory.AlignedFree(_ptr);
        _ptr = newPtr;
        _capacity = newCapacity;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            if (_ptr != null)
                NativeMemory.AlignedFree(_ptr);
            _ptr = null;
            _disposed = true;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowOutOfRange(int index)
        => throw new IndexOutOfRangeException($"Index {index} is out of range.");
}
