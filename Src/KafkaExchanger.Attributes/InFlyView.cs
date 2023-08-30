using System;
using System.Runtime.CompilerServices;

namespace KafkaExchanger
{
    public class InFlyView
    {
        private int _current;
        private readonly int _head;
        private readonly int _tail;
        private readonly int _size;

        private readonly Bucket[] _buckets;

        public InFlyView(
            Bucket[] buckets,
            int head,
            int tail,
            int current,
            int size
            )
        {
            _buckets = buckets;
            _head = head;
            _tail = tail;
            _current = current;
            _size = size;
        }

        public int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _size;
        }

        public int GlobalTailIndex
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _tail;
        }

        public int GlobalHeadIndex
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _head;
        }

        public int GlobalCurrentIndex
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _current;
        }

        public Bucket Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _buckets[_current];
        }

        public void ClusterView()
        {
            throw new NotImplementedException();
        }

        public Bucket AfterCurrentPeek()
        {
            throw new NotImplementedException();
        }

        public int CurrentIndexLocal()
        {
            if (_tail == _head)
            {
                return 0;
            }
            else
            if(_tail > _head)
            {
                return _current - _head;
            }
            else
            {
                if(_current == _head)
                {
                    return 0;
                }
                else
                if (_current > _head)
                {
                    return _current - _head;
                }
                else
                { 
                    return (_buckets.Length - _head) + _current;
                }
            }
        }

        public bool TryAdd(MessageInfo messageInfo)
        {
            if (_buckets[_current].HavePlace)
            {
                _buckets[_current].Add(messageInfo);
                return true;
            }

            if (!TryMoveNext())
            {
                return false;
            }

            _buckets[_current].Add(messageInfo);
            return true;
        }

        private bool TryMoveNext()
        {
            if (_size == 1)//_tail == _head
            {
                return false;
            }

            int newIndex;
            if (_tail > _head && _current + 1 > _tail)
            {
                newIndex = _head;
            }
            else
            {
                if (_current == _tail)
                {
                    newIndex = _head;
                }
                else if (_tail > _current)
                {
                    newIndex = _current + 1;
                    if (newIndex > _tail)
                    {
                        newIndex = _head;
                    }
                }
                else if (_tail < _current)
                {
                    newIndex = _current + 1;
                    if (newIndex == _buckets.Length)
                    {
                        newIndex = 0;
                    }
                }
                else
                {
                    throw new InvalidOperationException();
                }
            }

            if (_buckets[newIndex].HavePlace)
            {
                _current = newIndex;
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
